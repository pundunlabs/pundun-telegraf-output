package telegraf

// simpleoutput.go

import (
	"fmt"
	"github.com/erdemaksu/pundun"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs"
	"log"
	//Used for testing purposes
	//"reflect" //used to get TypeOf
)

type Pundun struct {
	Ok       bool
	Host     string
	User     string
	Password string
	Database string

	session pundun.Session
}

func (p *Pundun) Description() string {
	return "Configuration for Pundun"
}

func (p *Pundun) SampleConfig() string {
	return `
    ##Location of server in format host:port
    host = "localhost:8887"
    ##Credentials to connect to pundun
    user = "admin"
    password = "admin"
    #Table to push data to
    database = "telegraf"
    `
}

func (p *Pundun) Connect() error {
	// Make a connection to the URL here
	//    if p.Host == "" || p.Port == "" {
	//        return fmt.Errorf("Need to set host and port")
	//    }
	session, err := pundun.Connect(p.Host, p.User, p.Password)
	if err != nil {
		log.Println("E! Can't connect to server")
		//Return proper error
	}
	p.session = session
	//TODO Handle errors later
	//Don't know if it matters, but i think it is a good idea to try to create the Table at connect, then we sshould know that it exists when trying to write......
	keyDef := []string{"ts", "meas"}

	//Options should be configurable
	options := map[string]interface{}{
		"type": pundun.LeveldbTda,
		"tda": pundun.Tda{
			NumOfBuckets: 10,
			TimeMargin: pundun.TimeMargin{
				Unit:  pundun.Minutes,
				Value: 10,
			},
			TsField:   "ts",
			Precision: pundun.Nanosecond,
		},
		"data_model":         "array",
		"comparator":         "descending",
		"time_series":        false,
		"shards":             8,
		"distributed":        true,
		"replication_factor": 1,
	}

	res, err := pundun.CreateTable(p.session, p.Database, keyDef, options)
	if res != pundun.OK {
		resmap := res.(map[string]string)
		if resmap["system"] == "{error,\"table_exists\"}" {
			log.Printf("I! Error creating table: %v already exists", p.Database)
		} else {
			log.Printf("E! Error creating table: %v : %v\n", res, err)
		}

	}
	return nil
}

func (p *Pundun) Close() error {
	// Close connection to the URL here
	pundun.Disconnect(p.session)
	fmt.Printf("Disconnecting\n")
	return nil
}

func (p *Pundun) Write(metrics []telegraf.Metric) error {
	fmt.Println("Writing")
	for _, metric := range metrics {
		//TODO Think i should add tags to here
		fmt.Println(metric.Tags())
		key := map[string]interface{}{
			"ts":   metric.UnixNano(),
			"meas": metric.Name(),
		}

		//TODO Is this necessary? Maybe i just have to pass metric.Fields()
		columns := make(map[string]interface{})
		for k, v := range metric.Fields() {
			columns[k] = v
		}
		for k, v := range metric.Tags() {
			columns[k] = v
		}
		res, err := pundun.Write(p.session, p.Database, key, columns)
		if res != pundun.OK {
			log.Printf("E! R:%v:Err:%v\n", res, err)
		}

	}
	fmt.Printf("I! Wrote %v measurements to %v @ %v\n", len(metrics), p.Database, p.Host)
	return nil
}

func init() {
	outputs.Add("pundun", func() telegraf.Output { return &Pundun{} })
}
