package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/lib/pq"
)

type market struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Result  []struct {
		MarketCurrency     string      `json:"MarketCurrency"`
		BaseCurrency       string      `json:"BaseCurrency"`
		MarketCurrencyLong string      `json:"MarketCurrencyLong"`
		BaseCurrencyLong   string      `json:"BaseCurrencyLong"`
		MinTradeSize       float64     `json:"MinTradeSize"`
		MarketName         string      `json:"MarketName"`
		IsActive           bool        `json:"IsActive"`
		Created            string      `json:"Created"`
		Notice             interface{} `json:"Notice"`
		IsSponsored        interface{} `json:"IsSponsored"`
		LogoURL            string      `json:"LogoUrl"`
	} `json:"result"`
}

type ticker struct {
	market  string
	fecha   time.Time
	Success bool   `json:"success"`
	Message string `json:"message"`
	Result  struct {
		Bid  float64 `json:"Bid"`
		Ask  float64 `json:"Ask"`
		Last float64 `json:"Last"`
	} `json:"result"`
}
type orderbook struct {
	market  string
	fecha   time.Time
	Success bool   `json:"success"`
	Message string `json:"message"`
	Result  struct {
		Buy []struct {
			Quantity float64 `json:"Quantity"`
			Rate     float64 `json:"Rate"`
		} `json:"buy"`
		Sell []struct {
			Quantity float64 `json:"Quantity"`
			Rate     float64 `json:"Rate"`
		} `json:"sell"`
	} `json:"result"`
}

type orderhistory struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Result  []struct {
		ID        int     `json:"Id"`
		TimeStamp string  `json:"TimeStamp"`
		Quantity  float64 `json:"Quantity"`
		Price     float64 `json:"Price"`
		Total     float64 `json:"Total"`
		FillType  string  `json:"FillType"`
		OrderType string  `json:"OrderType"`
	} `json:"result"`
}
type marketsummaries struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Result  []struct {
		MarketName     string  `json:"MarketName"`
		High           float64 `json:"High"`
		Low            float64 `json:"Low"`
		Volume         float64 `json:"Volume"`
		Last           float64 `json:"Last"`
		BaseVolume     float64 `json:"BaseVolume"`
		TimeStamp      string  `json:"TimeStamp"`
		Bid            float64 `json:"Bid"`
		Ask            float64 `json:"Ask"`
		OpenBuyOrders  int     `json:"OpenBuyOrders"`
		OpenSellOrders int     `json:"OpenSellOrders"`
		PrevDay        float64 `json:"PrevDay"`
		Created        string  `json:"Created"`
	} `json:"result"`
}

var wg sync.WaitGroup
var db *sql.DB

func init() {
	var err error
	db, err = sql.Open("postgres", "postgres://broker:password@localhost/inversion?sslmode=disable")
	if err != nil {
		panic(err)
	}

	if err = db.Ping(); err != nil {
		panic(err)
	}
	fmt.Println("You connected to your database.")

}
func main() {
	var markets market
	url := "https://bittrex.com/api/v1.1/public/getmarkets"
	err := getjson(url, &markets)
	if err != nil {
		fmt.Printf("test error %v\n", err)
	} else {

		//gs := len(markets.Result) * 2
		wg.Add(2)
		getmarketsummaries()

		go forgetbookorder(markets)
		go forgetorderhistory(markets)

		wg.Wait()
		fmt.Println("Fin")

	}
}
func forgetbookorder(markets market) {
	concurrency := 10
	sem := make(chan bool, concurrency)
	for {
		for _, v := range markets.Result {
			sem <- true
			//getticker(v.MarketName)
			go getorderbook(v.MarketName, sem)
		}
		timer1 := time.NewTimer(time.Second * 1)
		<-timer1.C
	}
	for i := 0; i < cap(sem); i++ {
		sem <- true

	}
	wg.Done()
}
func forgetorderhistory(markets market) {
	concurrency := 10
	sem := make(chan bool, concurrency)
	println("forgetorderhistory")
	for {
		for _, v := range markets.Result {
			sem <- true
			//getticker(v.MarketName)

			go getorderhistory(v.MarketName, sem)
		}
		println("fin forgetorderhistory")
		timer1 := time.NewTimer(time.Second * 90)
		<-timer1.C
	}
	for i := 0; i < cap(sem); i++ {
		sem <- true

	}
	wg.Done()
}

func getjson(url string, result interface{}) error {

	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("cannot fetch URL %q: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected http GET status: %s", resp.Status)
	}
	// We could also check the resulting content type
	// here too.
	data, _ := ioutil.ReadAll(resp.Body)

	err = json.Unmarshal(data, result)
	if err != nil {
		return fmt.Errorf("cannot decode JSON: %v", err)
	}
	return nil
}

func getticker(market string) {
	url := "https://bittrex.com/api/v1.1/public/getticker?market=" + market

	var tmpticker ticker
	//fmt.Println(url)
	err := getjson(url, &tmpticker)
	if err != nil {
		fmt.Println(url, err)
	} else {

		tmpticker.market = market
		tmpticker.fecha = time.Now()
		_, err = db.Exec("INSERT INTO tickers (market, date, bid, ask,last) VALUES ($1, $2, $3, $4,$5)", tmpticker.market, tmpticker.fecha, tmpticker.Result.Bid, tmpticker.Result.Ask, tmpticker.Result.Last)
		if err != nil {
			fmt.Println(url, err)

		} else {
			fmt.Println(url, "ticket save to bd", market)
		}

	}
	//wg.Done()

}
func getorderbook(market string, sem <-chan bool) {
	defer func() { <-sem }()
	url := "https://bittrex.com/api/v1.1/public/getorderbook?market=" + market + "&type=both"

	var tmporderbook orderbook
	//fmt.Println(url)
	err := getjson(url, &tmporderbook)
	if err != nil {
		fmt.Println(url, err)
	} else {

		tmporderbook.market = market
		tmporderbook.fecha = time.Now()

		txn, err := db.Begin()
		if err != nil {
			fmt.Println(url, err)
		}

		stmt, err := txn.Prepare(pq.CopyIn("orderbook", "market", "date", "type", "quantity", "rate"))
		defer stmt.Close()
		if err != nil {
			fmt.Println(url, err)
			wg.Done()
			return
		}

		for _, book := range tmporderbook.Result.Buy {
			_, err = stmt.Exec(tmporderbook.market, tmporderbook.fecha, "BUY", book.Quantity, book.Rate)
			if err != nil {
				fmt.Println(url, err)
				wg.Done()
				return
			}
		}
		for _, book := range tmporderbook.Result.Sell {
			_, err = stmt.Exec(tmporderbook.market, tmporderbook.fecha, "SELL", book.Quantity, book.Rate)
			if err != nil {
				fmt.Println(url, err)
				wg.Done()
				return
			}
		}
		_, err = stmt.Exec()
		if err != nil {
			fmt.Println(url, err)
			wg.Done()
			return
		}

		err = txn.Commit()

		if err != nil {
			fmt.Println(url, err)
			wg.Done()
			return
		}
		fmt.Println(url, "orderbook save to bd", market)

	}
	//wg.Done()

}

func getmarketsummaries() {

	url := "https://bittrex.com/api/v1.1/public/getmarketsummaries"

	var tmp marketsummaries
	//fmt.Println(url)
	err := getjson(url, &tmp)
	if err != nil {
		fmt.Println(url, err)
	} else {

		txn, err := db.Begin()
		if err != nil {
			fmt.Println(url, err)
		}

		stmt, err := txn.Prepare(pq.CopyIn("marketsummaries", "ask", "basevolume", "bid", "created", "high", "last", "low", "market", "openbuyorders", "opensellorders", "prevday", "date"))
		defer stmt.Close()
		if err != nil {
			fmt.Println(url, err)
			wg.Done()
			return
		}

		for _, book := range tmp.Result {
			_, err = stmt.Exec(book.Ask, book.BaseVolume, book.Bid, book.Created, book.High, book.Last, book.Low, book.MarketName, book.OpenBuyOrders, book.OpenSellOrders, book.PrevDay, book.TimeStamp)
			if err != nil {
				fmt.Println(url, err)
				wg.Done()
				return
			}
		}

		_, err = stmt.Exec()
		if err != nil {
			fmt.Println(url, err)
			wg.Done()
			return
		}

		err = txn.Commit()

		if err != nil {
			fmt.Println(url, err)
			wg.Done()
			return
		}
		fmt.Println(url, "getmarketsummaries save to bd")

	}
	//wg.Done()

}

func getorderhistory(market string, sem <-chan bool) {
	defer func() { <-sem }()
	url := "https://bittrex.com/api/v1.1/public/getmarkethistory?market=" + market

	var tmporderhistory orderhistory
	//fmt.Println(url)
	err := getjson(url, &tmporderhistory)
	if err != nil {
		fmt.Println(url, err)
	} else {

		m := make(map[int]bool)
		var id []int
		for _, history := range tmporderhistory.Result {
			id = append(id, history.ID)

		}
		rows, err := db.Query(`SELECT id FROM orderhistory WHERE id = ANY($1)`, pq.Array(id))

		if err != nil {
			println(market, err)
			wg.Done()
			return
		}
		defer rows.Close()

		for rows.Next() {
			id := 0
			err := rows.Scan(&id)
			if err != nil {
				fmt.Println(url, err)

			} else {
				m[id] = true

			}
		}
		txn, err := db.Begin()
		if err != nil {
			fmt.Println(url, err)
			wg.Done()
			return
		}

		stmt, err := txn.Prepare(pq.CopyIn("orderhistory", "id", "market", "date", "type", "quantity", "price", "total", "filltype", "ordertype"))
		if err != nil {
			fmt.Println(url, err)
			wg.Done()
			return
		}
		defer stmt.Close()
		for _, history := range tmporderhistory.Result {
			if _, exists := m[history.ID]; !exists {
				_, err = stmt.Exec(history.ID, market, history.TimeStamp, history.OrderType, history.Quantity, history.Price, history.Total, history.FillType, history.OrderType)
				if err != nil {
					fmt.Println(url, err)
					wg.Done()
					return
				}
			}

		}

		_, err = stmt.Exec()
		if err != nil {
			fmt.Println(url, err)
			wg.Done()
			return
		}

		err = txn.Commit()
		if err != nil {
			fmt.Println(url, err)
			wg.Done()
			return
		}
		fmt.Println(url, "tmporderhistory save to bd", market)

	}
	//wg.Done()

}
