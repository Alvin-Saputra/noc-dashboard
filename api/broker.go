package api

type Broker struct {
	Notifier chan []byte

	newClients     chan chan []byte    
	closingClients chan chan []byte      
	clients        map[chan []byte]bool  
}

func NewBroker() *Broker {
	broker := &Broker{
		Notifier:       make(chan []byte, 100),
		newClients:     make(chan chan []byte),
		closingClients: make(chan chan []byte),
		clients:        make(map[chan []byte]bool),
	}

	go broker.listen()
	return broker
}

func (b *Broker) listen() {
	for {
		select {
		case s := <-b.newClients:
			b.clients[s] = true
			
		case s := <-b.closingClients:
			delete(b.clients, s)
			
		case event := <-b.Notifier:
	
			for clientMessageChan := range b.clients {
				select {
				case clientMessageChan <- event:
					
				default:
		
				}
			}
		}
	}
}