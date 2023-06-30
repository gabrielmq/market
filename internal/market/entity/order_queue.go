package entity

type OrderQueue struct {
	Orders []*Order
}

func NewOrderQueue() *OrderQueue {
	return &OrderQueue{}
}

func (o *OrderQueue) Less(i, j int) bool {
	return o.Orders[i].Price < o.Orders[j].Price
}

func (o *OrderQueue) Swap(i, j int) {
	o.Orders[i], o.Orders[j] = o.Orders[j], o.Orders[i]
}

func (o *OrderQueue) Len() int {
	return len(o.Orders)
}

func (o *OrderQueue) Push(value interface{}) {
	o.Orders = append(o.Orders, value.(*Order))
}

func (o *OrderQueue) Pop() interface{} {
	old := o.Orders
	n := len(old)
	item := old[n-1]
	o.Orders = old[0 : n-1]
	return item
}
