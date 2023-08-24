package dag

import (
	"fmt"
)

// Vertex type implements a vertex of a Directed Acyclic graph or DAG.
type Vertex struct {
	ID       int
	Value    interface{}
	Parents  *OrderedSet
	Children *OrderedSet
}

// NewVertex creates a new vertex.
func NewVertex(id int, value interface{}) *Vertex {
	return &Vertex{
		ID:       id,
		Parents:  NewOrderedSet(),
		Children: NewOrderedSet(),
		Value:    value,
	}

}

// Degree 出度入度总和
func (v *Vertex) Degree() int {
	return v.InDegree() + v.OutDegree()
}

// InDegree 入度
func (v *Vertex) InDegree() int {
	return v.Parents.Size()
}

// OutDegree 出度
func (v *Vertex) OutDegree() int {
	return v.Children.Size()
}

// String 返回Vertex信息
func (v *Vertex) String() string {
	return fmt.Sprintf("ID: %d - Parents: %d - Children: %d - Value: %v\n", v.ID, v.Parents.Size(), v.Children.Size(), v.Value)
}
