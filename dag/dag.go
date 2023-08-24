package dag

import (
	"fmt"
	"sync"
)

var (
	VertexNotExistsErr = func(id interface{}) error {
		return fmt.Errorf("vertex %v not found in the graph%w", id, VNEErr)
	}
	VNEErr = fmt.Errorf("")
)

// DAG type implements a Directed Acyclic Graph data structure.
type DAG struct {
	mu       sync.Mutex
	vertices OrderedMap
}

// NewDAG creates a new Directed Acyclic Graph or DAG.
func NewDAG() *DAG {
	d := &DAG{
		vertices: *NewOrderedMap(),
	}

	return d
}

// AddVertex adds a vertex to the graph.
func (d *DAG) AddVertex(v *Vertex) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.vertices.Put(v.ID, v)
}

// DeleteVertex deletes a vertex and all the edges referencing it from the
// graph.
func (d *DAG) DeleteVertex(vertex *Vertex) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.vertices.Contains(vertex.ID) {
		d.vertices.Remove(vertex.ID)
		return nil
	}

	return fmt.Errorf("vertex with ID %v not found", vertex.ID)
}

// AddEdge adds a directed edge between two existing vertices to the graph.
// tailVertex -> headVertex
func (d *DAG) AddEdge(tailVertex, headVertex *Vertex) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if !d.vertices.Contains(headVertex.ID) {
		return fmt.Errorf("vertex with ID %v not found", headVertex.ID)
	}
	if !d.vertices.Contains(tailVertex.ID) {
		return fmt.Errorf("vertex with ID %v not found", tailVertex.ID)
	}

	// Check if edge already exists.
	if tailVertex.Children.Contains(headVertex) {
		return fmt.Errorf("edge (%v,%v) already exists", tailVertex.ID, headVertex.ID)
	}

	// Add edge.
	tailVertex.Children.Add(headVertex)
	headVertex.Parents.Add(tailVertex)

	return nil
}

// DeleteEdge deletes a directed edge between two existing vertices from the
// graph.
func (d *DAG) DeleteEdge(tailVertex, headVertex *Vertex) error {
	if tailVertex.Children.Contains(headVertex) {
		tailVertex.Children.Remove(headVertex)
	}
	if headVertex.Parents.Contains(tailVertex) {
		headVertex.Parents.Remove(tailVertex)
	}

	return nil
}

// GetVertex return a vertex from the graph given a vertex ID.
func (d *DAG) GetVertex(id interface{}) (*Vertex, error) {
	v, found := d.vertices.Get(id)
	if !found {
		return nil, VertexNotExistsErr(id)
	}

	return v.(*Vertex), nil
}

func (d *DAG) GetAllVertex() []*Vertex {
	values := d.vertices.Values()
	vertexes := make([]*Vertex, len(values))
	for i, vertex := range values {
		vertexes[i] = vertex.(*Vertex)
	}
	return vertexes
}

// Order return the number of vertices in the graph.
func (d *DAG) Order() int {
	return d.vertices.Size()
}

// Size return the number of edges in the graph.
func (d *DAG) Size() int {
	var numEdges int
	for _, vertex := range d.vertices.Values() {
		numEdges += vertex.(*Vertex).Children.Size()
	}

	return numEdges
}

// SinkVertices return vertices with no children defined by the graph edges.
func (d *DAG) SinkVertices() []*Vertex {
	sinkVertices := make([]*Vertex, 0)

	for _, vertex := range d.vertices.Values() {
		if vertex.(*Vertex).Children.Size() == 0 {
			sinkVertices = append(sinkVertices, vertex.(*Vertex))
		}
	}

	return sinkVertices
}

// SourceVertices return vertices with no parent defined by the graph edges.
func (d *DAG) SourceVertices() []*Vertex {
	sourceVertices := make([]*Vertex, 0)

	for _, vertex := range d.vertices.Values() {
		if vertex.(*Vertex).Parents.Size() == 0 {
			sourceVertices = append(sourceVertices, vertex.(*Vertex))
		}
	}

	return sourceVertices
}

// Successors return vertices that are children of a given vertex.
func (d *DAG) Successors(vertex *Vertex) ([]*Vertex, error) {
	successors := make([]*Vertex, len(vertex.Children.Values()))

	_, found := d.GetVertex(vertex.ID)
	if found != nil {
		return successors, fmt.Errorf("vertex %d not found in the graph", vertex.ID)
	}

	for i, v := range vertex.Children.Values() {
		successors[i] = v.(*Vertex)
	}

	return successors, nil
}

// Predecessors return vertices that are parent of a given vertex.
func (d *DAG) Predecessors(vertex *Vertex) ([]*Vertex, error) {
	predecessors := make([]*Vertex, len(vertex.Parents.Values()))

	_, found := d.GetVertex(vertex.ID)
	if found != nil {
		return predecessors, fmt.Errorf("vertex %d not found in the graph", vertex.ID)
	}

	for i, v := range vertex.Parents.Values() {
		predecessors[i] = v.(*Vertex)
	}

	return predecessors, nil
}

// String implements stringer interface.
//
// Prints a string representation of this instance.
func (d *DAG) String() string {
	result := fmt.Sprintf("DAG Vertices: %d - Edges: %d\nVertices:\n", d.Order(), d.Size())
	for _, vertex := range d.GetAllVertex() {
		result += vertex.String()
	}

	return result
}

// ValidateCycle
// 判断dag图是否有环
func (d *DAG) ValidateCycle() (bool, error) {
	queue := make([]*Vertex, 0)
	inDegree := make(map[int]int)
	for _, vertex := range d.GetAllVertex() {
		inDegree[vertex.ID] = vertex.InDegree()
		if vertex.InDegree() == 0 {
			queue = append(queue, vertex)
		}
	}
	if len(queue) == 0 { // 入度为0的节点不存在，肯定有环
		return true, nil
	}
	var cnt int
	for len(queue) > 0 {
		vertex := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		cnt++
		successors, err := d.Successors(vertex)
		if err != nil {
			return false, err
		}
		queue = loopPatches(successors, inDegree, queue)
	}

	return cnt != len(d.vertices.store), nil
}

func loopPatches(vertices []*Vertex, inDegree map[int]int, queue []*Vertex) []*Vertex {
	for _, v := range vertices {
		vertexT := v
		degree, ok := inDegree[vertexT.ID]
		if !ok {
			continue
		}
		degree--
		if degree == 0 {
			queue = append(queue, vertexT)
		}
		inDegree[vertexT.ID] = degree
	}
	return queue
}
