package jobscheduler

import (
	"context"
	"errors"
	"fmt"
	"github.com/tangvis/job-scheduler/dag"
	"runtime"
	"strings"
	"sync"
	"time"

	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

const (
	lockKey          = "lock_key"
	defaultMaxWorker = 10000
	defaultTimeout   = 10
)

// GigaI
// 不同的业务都可以实现自己的giga来使用编排框架
type GigaI interface {
	Ctx() context.Context
	Err() error
	Cancel()
	AppendError(err error, name string)
	RegisterProcessor(processor ...*Processor)
	GetProcessor(idx int) *Processor
	GetAllProcessors() map[int]*Processor
	ErrGroup() *errgroup.Group
	ConcurrencyRun(p *Processor)
	WaitingQueue() chan *Processor
	AddWaitingProcessor(p *Processor)
	SetWaitingQueue()

	BizData() interface{}
	Name() string
}

// RunGiga
// 启动giga
func RunGiga(giga GigaI) error {
	defer func() {
		if giga.Err() != nil {
			fmt.Printf("RunGiga Error: %s", giga.Err().Error())
		}
	}()
	go func() {
		for _, processor := range giga.GetAllProcessors() {
			if !processor.NoDependence() {
				continue
			}
			giga.AddWaitingProcessor(processor)
		}
	}()
	if err := loopWaitingProcessorQueue(giga); err != nil {
		return err
	}

	return giga.ErrGroup().Wait()
}

func loopWaitingProcessorQueue(giga GigaI) error {
	for {
		select {
		case <-giga.Ctx().Done():
			return giga.Ctx().Err()
		case waitingProcessor, ok := <-giga.WaitingQueue():
			if !ok { // 如果channel关闭，则退出循环
				return nil
			}
			giga.ConcurrencyRun(waitingProcessor)
		}
	}
}

// BaseGiga
// 作业执行周期内的上下文
// 可以理解成一个工厂，工人就是processor，工人需要的工具、原材料以及生产的成品都在giga里面，工人不需要对工厂外有任何联系
// giga需要提供的功能就是：作业所需的🔧、原料以及组装成品，外界沟通的代理：日志、打点等
// 业务无关，具体的模块可以根据自己需求实现giga，继承base giga
type BaseGiga struct {
	ctx    context.Context
	cancel func()

	errGroup *errgroup.Group
	weighted *semaphore.Weighted
	// channel大小为remainProcessors
	// 设置为这个大小的原因是为了防止死锁，因为我们chan的生产方有两个，一个是初始启动的时候入度为0的节点会被放进chan
	// 第二个是我们的processor处理完成后会检查后继节点是不是入度变为0了，是的话就放进chan
	// 同时消费端受到最大协程数配置的限制，而生产端的processor正是协程数量的占用方，因此为了保证我们的processor
	// 一定能放入chan，我们初始化为processor的总数量
	processorQueue   chan *Processor
	remainProcessors atomic.Int64 // 剩余未执行的processor个数

	processors map[int]*Processor

	err error
	// mutexes锁池
	mutexes sync.Map
}

func (giga *BaseGiga) Ctx() context.Context {
	return giga.ctx
}

func (giga *BaseGiga) Err() error {
	return giga.err
}

func (giga *BaseGiga) GetProcessor(idx int) *Processor {
	return giga.processors[idx]
}

func (giga *BaseGiga) RegisterProcessor(processor ...*Processor) {
	for _, p := range processor {
		giga.processors[p.id] = p
		giga.remainProcessors.Add(1)
	}
}

func (giga *BaseGiga) GetAllProcessors() map[int]*Processor {
	return giga.processors
}

func (giga *BaseGiga) ErrGroup() *errgroup.Group {
	return giga.errGroup
}

func (giga *BaseGiga) WaitingQueue() chan *Processor {
	return giga.processorQueue
}

func (giga *BaseGiga) AddWaitingProcessor(p *Processor) {
	giga.processorQueue <- p
	if giga.remainProcessors.Dec() == 0 {
		close(giga.processorQueue)
	}
}

func (giga *BaseGiga) SetWaitingQueue() {
	giga.processorQueue = make(chan *Processor, giga.remainProcessors.Load())
}

// ConcurrencyRun
// 并发执行processor
func (giga *BaseGiga) ConcurrencyRun(p *Processor) {
	// 获取协程许可
	err := giga.weighted.Acquire(giga.ctx, 1)
	if err != nil {
		p.Abort(err)
	}
	giga.ErrGroup().Go(func() error {
		defer func() {
			if ev := recover(); ev != nil {
				p.Abort(fmt.Errorf("panic"))
				stack := make([]byte, 16*1024)
				_ = runtime.Stack(stack, false)
				fmt.Printf("[PANIC]%+v, %s\n", ev, stack)
				return
			}
			giga.weighted.Release(1)
		}()
		// 执行拦截器链
		p.exe()
		return giga.Err()
	})

}

func NewBaseGiga(originCtx context.Context, config JobConfig) BaseGiga {
	if config.MaxWorker == 0 { // 没有传就默认不限制协程数量
		config.MaxWorker = defaultMaxWorker
	}
	if config.Timeout == 0 {
		config.Timeout = defaultTimeout
	}
	timeoutCtx, timeoutCancel := context.WithTimeout(originCtx, config.Timeout*time.Second)
	g, ctx := errgroup.WithContext(timeoutCtx)
	ctx, cancel := context.WithCancel(ctx)
	return BaseGiga{
		ctx: ctx,
		cancel: func() {
			cancel()
			timeoutCancel()
		},
		err:        nil,
		errGroup:   g,
		weighted:   semaphore.NewWeighted(config.MaxWorker),
		processors: map[int]*Processor{},
	}
}

func (giga *BaseGiga) Cancel() {
	if giga.cancel != nil {
		giga.cancel()
	}
}

func (giga *BaseGiga) AppendError(err error, name string) {
	mutex, _ := giga.mutexes.LoadOrStore(lockKey, &sync.Mutex{})
	mutex.(*sync.Mutex).Lock()
	defer mutex.(*sync.Mutex).Unlock()
	wrappedErr := fmt.Errorf("processor:%s, err:%w", name, err)
	if giga.err != nil {
		giga.err = fmt.Errorf("%w;%s", giga.err, wrappedErr.Error())
		return
	}
	giga.err = wrappedErr
}

type Invoker func(giga GigaI) error

func (i Invoker) Invoke(giga GigaI) error {
	return i(giga)
}

// InvokerGroup 未来扩展特性，多个invoker也可以组成一个group组装进同一个processor
type InvokerGroup []Invoker

func (group InvokerGroup) ConvToInvoker() Invoker {
	return func(giga GigaI) error {
		for _, invoker := range group {
			if err := invoker(giga); err != nil {
				return err
			}
		}
		return nil
	}
}

// Processor
// 作业执行器
// 可以理解成工人，可以在giga进行各种加工和生产动作
type Processor struct {
	giga GigaI

	unaryIntChain UnaryIntChain // 拦截器链，最后一个元素固定是实际的业务处理函数
	intIndex      int           // 拦截器索引

	notifier     *Notifier   // 当前节点的通知器
	depNotifiers []*Notifier // 下游节点notifier列表

	RetryManager RequestRetryManager

	name string
	id   int

	err error
}

func (p *Processor) Reset(giga GigaI, id int, name string) {
	p.giga = giga
	p.unaryIntChain = make([]UnaryProcessorInterceptor, 0)
	p.intIndex = -1
	p.notifier.Reset()
	p.depNotifiers = p.depNotifiers[:0]
	p.RetryManager = nil
	p.name = name
	p.id = id
	p.err = nil
}

func (p *Processor) Release() {
	ResourcePool.ProcessorPool.Put(p)
}

func GetProcessorFromPool(giga GigaI, id int, name string) *Processor {
	processor := ResourcePool.ProcessorPool.Get().(*Processor)
	processor.Reset(giga, id, name)
	return processor
}

func NewProcessor(giga GigaI, id int, name string) *Processor {
	p := &Processor{
		giga:     giga,
		intIndex: -1,
		name:     name,
		id:       id,
		notifier: NewNotifier(),
	}
	p.notifier.SetProcessor(p)
	return p
}

// SetUpstream
// @Description: 设置上游节点
func (p *Processor) SetUpstream(processor *Processor) {
	processor.RegisterDepNotifiers(p.notifier)
	p.notifier.Inc()
}

// RegisterInterceptor
// @Description: 注册拦截器
func (p *Processor) RegisterInterceptor(int ...UnaryProcessorInterceptor) {
	// 后注册的拦截器先执行
	p.unaryIntChain = append(int, p.unaryIntChain...)
}

// RegisterCoreExecutor
// @Description: 注册核心处理函数
func (p *Processor) RegisterCoreExecutor(invokers ...Invoker) {
	invoker := InvokerGroup(invokers).ConvToInvoker()
	p.unaryIntChain = append(p.unaryIntChain,
		func(processor *Processor) {
			var err error
			// 重试逻辑
			for {
				err = invoker(processor.giga)
				if p.RetryManager == nil {
					break
				}
				if err == nil || !p.RetryManager(p.giga, err) {
					break
				}
			}
			if err != nil {
				processor.Abort(err)
			}
		})
}

// Exe
// @Description: 执行
func (p *Processor) Exe(giga GigaI) {
	select {
	// giga的cancel信号
	case <-giga.Ctx().Done():
		// 设置context超时为当前processor的错误
		p.err = giga.Ctx().Err()
		return
	default:
	}
	giga.ConcurrencyRun(p)
}

func (p *Processor) exe() {
	p.Next()
	// 如果整个giga有错误，直接退出
	if p.giga.Err() != nil {
		return
	}
	// 通知下游
	p.Notify()
}

// Abort
// @Description: 中断整个giga的执行
// @param err: 用来设置当前processor的错误以及giga的错误
func (p *Processor) Abort(err error) {
	p.giga.Cancel()
	if err != nil {
		p.giga.AppendError(err, p.name)
		p.err = err
	}
}

// RegisterDepNotifiers
// @Description: 注册下游节点的通知器
func (p *Processor) RegisterDepNotifiers(notifiers ...*Notifier) {
	p.depNotifiers = append(notifiers, p.depNotifiers...)
}

// Notify
// @Description: 依赖通知，如果有其他processor依赖本processor的处理，通过这个接口进行通知
func (p *Processor) Notify() {
	for _, notifier := range p.depNotifiers {
		notifier.Dec()
	}
}

// NoDependence
// @Description: 当前节点是否没有上游依赖
func (p *Processor) NoDependence() bool {
	return p.notifier.NoDependence()
}

// Next
// same as gin
// @Description: 执行下一个拦截器
func (p *Processor) Next() {
	p.intIndex++
	for p.intIndex < len(p.unaryIntChain) {
		p.unaryIntChain[p.intIndex](p)
		p.intIndex++
	}
}

type UnaryProcessorInterceptor func(p *Processor)
type UnaryIntChain []UnaryProcessorInterceptor

type Notifier struct {
	processor *Processor
	degree    *atomic.Int64
}

// Dec
// @Description: 当前通知器的度-1，如果度为0，则放入待启动队列
func (notifier *Notifier) Dec() {
	if notifier.degree.Dec() == 0 {
		notifier.processor.giga.AddWaitingProcessor(notifier.processor)
	}
}

// Inc
// @Description: 当前通知器的度+1
func (notifier *Notifier) Inc() {
	notifier.degree.Inc()
}

// NoDependence
// @Description: 当前通知器的度是否==0
func (notifier *Notifier) NoDependence() bool {
	return notifier.degree.Load() == 0
}

func (notifier *Notifier) Reset() {
	notifier.degree = atomic.NewInt64(0)
}

func (notifier *Notifier) SetProcessor(p *Processor) {
	notifier.processor = p
}

func NewNotifier() *Notifier {
	return &Notifier{}
}

var interceptorFactory = NewInterceptorFactory()

// BuildScheduleJobs
// @Description: 从dag组装编排作业
// @param ctx
// @param dag 解析好的dag图
// @return *Giga giga工厂
// @return error
// nolint
func BuildScheduleJobs(giga GigaI, dag *dag.DAG, bizFuncCollection map[string]Invoker) (GigaI, func(), error) {
	toReleaseProcessors := make([]*Processor, 0)
	releaseFunc := func() {
		for _, processor := range toReleaseProcessors {
			if processor != nil {
				processor.Release()
			}
		}
	}
	isCycle, err := dag.ValidateCycle()
	if err != nil {
		return nil, releaseFunc, err
	}
	if isCycle {
		return nil, releaseFunc, DagHasCycleErr
	}
	for _, vertex := range dag.GetAllVertex() {
		// 创建processor
		config, ok := vertex.Value.(ConfigEntity)
		if !ok {
			return nil, releaseFunc, JobConfigNotLegalErr(vertex.Value)
		}
		funcGroup := config.FuncGroup
		p := GetProcessorFromPool(giga, vertex.ID, strings.Join([]string{giga.Name(), strings.Join(funcGroup, "->")}, ":"))
		toReleaseProcessors = append(toReleaseProcessors, p)
		// 注册processor
		giga.RegisterProcessor(p)
		// 获取processor对应的业务处理函数
		var fs InvokerGroup
		for _, funcName := range funcGroup {
			f, ok := bizFuncCollection[funcName]
			if !ok {
				return nil, releaseFunc, JobFuncNotFoundErr(vertex.Value.(string))
			}
			fs = append(fs, f)
		}
		// 注册业务处理函数
		p.RegisterCoreExecutor(fs...)
		ProcessorRegisterInterceptors(p, config)
	}
	giga.SetWaitingQueue() // 在processor全部注册完成之后，创建waiting queue，目的是waiting queue的大小需要大于等于processor的个数
	for _, vertex := range dag.GetAllVertex() {
		// 从giga获取已注册的processor
		p := giga.GetProcessor(vertex.ID)
		// 从dag图获取processor的上游依赖节点
		predecessors, err := dag.Predecessors(vertex)
		if err != nil {
			return nil, releaseFunc, err
		}
		for _, predecessor := range predecessors {
			// 依次注册上游依赖节点
			p.SetUpstream(giga.GetProcessor(predecessor.ID))
		}
	}
	return giga, releaseFunc, nil
}

// ProcessorRegisterInterceptors
// 注册其他拦截器（日志，打点等）
// todo 通过map配置化
func ProcessorRegisterInterceptors(p *Processor, config ConfigEntity) {
	if !config.IgnoreCat {
		p.RegisterInterceptor(interceptorFactory.NewCatInterceptor())
	}
	if !config.IgnoreLog {
		p.RegisterInterceptor(interceptorFactory.NewLogInterceptor())
	}
}

func ParseConfig(ctx context.Context, config JobConfig) (*dag.DAG, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	d := dag.NewDAG()
	for _, configEntity := range config.Configs {
		d.AddVertex(dag.NewVertex(configEntity.ID, configEntity))
	}
	for _, configEntity := range config.Configs {
		for _, prev := range configEntity.Prev {
			tail, err := d.GetVertex(prev)
			if errors.Is(err, dag.VNEErr) {
				fmt.Printf("prev processor id (%d) not exists\n", prev)
				continue
			}
			if err != nil {
				return nil, err
			}
			head, err := d.GetVertex(configEntity.ID)
			if err != nil {
				return nil, err
			}
			if err = d.AddEdge(tail, head); err != nil {
				return nil, err
			}
		}
	}
	return d, nil
}

func validateConfig(config JobConfig) error {
	if len(config.Configs) == 0 {
		return EmptyConfigErr
	}
	m := make(map[int]struct{})
	for _, configEntity := range config.Configs {
		m[configEntity.ID] = struct{}{}
	}
	if len(m) != len(config.Configs) {
		return IDDuplicatedErr
	}
	return nil
}
