package coinApi

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
)

// This file was generated from JSON Schema using quicktype, do not modify it directly.
// To parse and unparse this JSON data, add this code to your project and do:
//
//    coinAPI, err := UnmarshalCoinAPI(bytes)
//    bytes, err = coinAPI.Marshal()


func UnmarshalCoinAPI(data []byte) (CoinAPI, error) {
	var r CoinAPI
	err := json.Unmarshal(data, &r)
	return r, err
}

func (r *CoinAPI) Marshal() ([]byte, error) {
	return json.Marshal(r)
}
//The structs defined to convert the json response from the API to usable golang types
type CoinAPI struct {
	Coins []Coin `json:"data"`
}

type Coin struct {
	ID                string `json:"id"`               
	Rank              string `json:"rank"`             
	Symbol            string `json:"symbol"`           
	Name              string `json:"name"`             
	Supply            string `json:"supply"`           
	MaxSupply         string `json:"maxSupply"`        
	MarketCapUsd      string `json:"marketCapUsd"`     
	VolumeUsd24Hr     string `json:"volumeUsd24Hr"`    
	PriceUsd          string `json:"priceUsd"`         
	ChangePercent24Hr string `json:"changePercent24Hr"`
	Vwap24Hr          string `json:"vwap24Hr"`         
	Explorer          string `json:"explorer"`         
}


/*
* Queries the API and converts the json response.
* Sends back the array of coin data (100 coins in total) to the calling function
*/
func GetAllCoins() []Coin {
	url := "https://api.coincap.io/v2/assets";

	req, _ := http.NewRequest("GET", url, nil);

	res, _ := http.DefaultClient.Do(req);

	responseData, err := ioutil.ReadAll(res.Body);
	if err != nil {
		log.Println("Error while reading response body", err.Error());
	}
	defer res.Body.Close();
	responseCoinData, err := UnmarshalCoinAPI(responseData);
	if err != nil{
		log.Println("Error while unmarshalling coin api response", err.Error());
	}
	
	return responseCoinData.Coins;
}