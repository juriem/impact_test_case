package main


import (
	"github.com/streadway/amqp"
	"fmt"
)

const (
	INCOME_QUEUE = "interest-queue"
	OUTCOME_QUEUE = "solved-interest-queue"
)

func main() {

	conn, err := amqp.Dial("amqp://myjar:myjar@impact.ccat.eu")
	defer conn.Close()
	if err != nil {
		panic(err)
	}

	channel, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	// Declare
	incomeQueue, err := channel.QueueDeclare(INCOME_QUEUE, false, true, false, false, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(incomeQueue)

	outcomeQueue, err := channel.QueueDeclare(OUTCOME_QUEUE, true, false, false, false, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(outcomeQueue)

	if err := channel.ExchangeDeclare("", "direct", true, false, false, true, nil); err != nil {
		panic(err)
	}

	if err := channel.QueueBind(incomeQueue.Name, "", "", true, nil); err != nil {
		panic(err)
	}

	deliveries, err := channel.Consume(incomeQueue.Name, "", false, false, false, true, nil)
	if err != nil {
		panic(err)
	}

	for {
		d := <-deliveries

		fmt.Printf(
			"got %dB delivery: [%v] %q\n",
			len(d.Body),
			d.DeliveryTag,
			d.Body,
		)
		d.Ack(true)
	}




	//	queue, _, err := channel.Get("interest-queue", true)
	//	if err != nil {
	//		panic(err)
	//	}

	fmt.Println("Ok")



}
