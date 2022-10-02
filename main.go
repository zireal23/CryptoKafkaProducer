package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/zirael23/CryptoKafkaProducer/coinApi"
	kafkaSchemapb "github.com/zirael23/CryptoKafkaProducer/kafkaSchema"
	"google.golang.org/protobuf/proto"
)

/*
* The entry point of the application.
* Loading the configurations from the enviroment variables.
* Also initilaising the kafka producer API.
 */

func main(){
// 	if(os.Getenv("GO_ENV")!="production"){
// 	err := godotenv.Load();

// 	if err != nil {
// 		log.Println("Error while loading env file", err.Error());
// 	}
// }
	
	//create a new producer
	fmt.Println(os.Getenv("SASLMECHANISM"));
	producer, err := initialiseKafkaProducer();
	if err != nil {
		log.Println("Error while initialising producer: ", err.Error());
		os.Exit(1);
	}
	
	queryAPIandPublishMessage(producer);	


}
/*
* The implmentation uses Shopify's Sarama Library.
* The Sarama Library has a nice kafka producer API implmentation.
* To create a producer, first setup the sarama config struct using the NewConfig function.
* The commented configs were for the confluent kafka but since I am using a containerized kafka instance
* they are no longer needed.
* The Newproducer function (has both sync/async variants) is used to initialise the producer
* It takes in the config struct and the kafka connection address that is defined in the docker compose file.
* The loop is for retrying the connection until it is connected to the kafka instance
*/


func initialiseKafkaProducer() (sarama.SyncProducer, error){
	// setup sarama log to stdout
	sarama.Logger = log.New(os.Stdout, "", log.Ltime);

	//producer config
	config := sarama.NewConfig();
	kafkaConnectionURL := os.Getenv("kafkaConnectionURL");
	saslMechanism := os.Getenv("SASLMECHANISM");
	saslUserName := os.Getenv("SASLUSER");
	saslPassword := os.Getenv("SASLPASSWORD");
	clientID := os.Getenv("CLIENTID");

	config.Net.SASL.Enable = true;
	config.Net.TLS.Enable = true;
	config.Net.SASL.Mechanism = sarama.SASLMechanism(saslMechanism);
	config.Net.SASL.User = saslUserName;
	config.Net.SASL.Password = saslPassword;
	config.ClientID = clientID;
	//Max number of retries while pushing message to kafka
	config.Producer.Retry.Max = 5;
	// The maximum duration the broker will wait the receipt of the number of
	// RequiredAcks (defaults to 10 seconds). This is only relevant when
	// RequiredAcks is set to WaitForAll or a number > 1. Only supports
	// millisecond resolution, nanoseconds will be truncated. Equivalent to
	// the JVM producer's `request.timeout.ms` setting.
	config.Producer.RequiredAcks = sarama.WaitForAll;
	//If enabled, successfully delivered messages will be returned on the Successes channel (default disabled).
	config.Producer.Return.Successes = true;

	//async producer 
	// prd, err := sarama.NewAsyncProducer([]string{kafkaConn}, config);

	//sync producer
	//FIXME: SyncProducer publishes Kafka messages, blocking until they have been acknowledged. It routes messages to the correct broker, refreshing metadata as appropriate, and parses responses for errors. Must call Close() on a producer to avoid leaks, it may not be garbage-collected automatically when it passes out of scope.

	var producer sarama.SyncProducer;
	var err error;
	//Commented because this one is used for docker kafka
	//kafkaConnectionURL := os.Getenv("KAFKA_CONNECTION");
	for{
		producer, err = sarama.NewSyncProducer([]string{kafkaConnectionURL}, config);
		if err == nil {
			break;
		}
		log.Println("Couldnt connect to kafka, Retrying....", err.Error());
		time.Sleep(time.Second*10);
	}

	return producer, err;
}

/*
* The function queries the public API at intervals of 100*100 ms = 10 seconds, couldnt reduce the interval as it was returning
* duplicate values only. Need to find better free/paid API later.
* The Data is then formatted according to the schema defined in the proto file.
* Atlast the data is sent to the message queue.
*/

func queryAPIandPublishMessage(producer sarama.SyncProducer){
	//FIXME: Delete later when benchmarking is completed
	startTime := time.Now();
	for {
		coins := coinApi.GetAllCoins();
		for _, currentCoin  := range coins{
			kafkaMessage := createMessageFormat(currentCoin);
			publishMessage(kafkaMessage, producer,currentCoin);
			time.Sleep(time.Millisecond* 100);
		}
		log.Println("The entire process took: ",time.Since(startTime).Seconds());
	}

}

/*
* Takes in the details of the current coin response.
* Creates a message adhering to the schema defined.
* Converts/marshals the message to a byte slice so that it can be sent to the message queue.
* The byte slice is returned to the calling function(queryAPIandPublishMessage).
*/

func createMessageFormat(coinData coinApi.Coin) []byte {
	//The response from the API returns the price as a string, so converting it into float type.
	coinPrice, err  := strconv.ParseFloat(coinData.PriceUsd, 32);
	if err != nil{
		log.Println("Error while converting price to float", err.Error());
	}
	//Building up the message following the schema definitions
	message := &kafkaSchemapb.CoinData{
		Id: coinData.ID,
		Name: coinData.Name,
		Price: float32(coinPrice),
		Timestamp: time.Now().Unix(),
	}
	//Convertig the message to a byet slice/array.
	kafkaMessage, err := proto.Marshal(message);
	if err != nil {
		log.Println("Error while serializing message", err.Error());
	} 
	return kafkaMessage;
}

/*
* The publishMessage function takes in the byte slice created earlier, the producer interface and the specific cryptocurrency whose
* data we are pushing to the message queue right now.
* ProducerMessage is the collection of elements passed to the Producer in order to send a message.
* Takes in the name of the topic we want to write our message to, the key to uniquely identify the message, here I am using the
* coinID as a key that is needed to be converted to bytes from string, and the message byte slice as well.
*/


func publishMessage(message []byte, producer sarama.SyncProducer, coin coinApi.Coin) {
	kafkaTopic := os.Getenv("KAFKA_TOPIC");
	msg := &sarama.ProducerMessage{
		Topic: kafkaTopic,
		Key: sarama.StringEncoder(coin.ID),
		Value: sarama.ByteEncoder(message),
	}
	//SendMessage function sends the message and returns the parition number and the offset at which the message was published and
	// error which is not nil in case the message wasnt pushed successfully
	messagePartition, messageOffset, err := producer.SendMessage(msg);
	if err != nil {
		log.Println("Error pushing message to Kafka", err.Error());
	}

	fmt.Println("Message pushed to partition: ", messagePartition);
	fmt.Println("Message pushed to offset: ", messageOffset);

}
