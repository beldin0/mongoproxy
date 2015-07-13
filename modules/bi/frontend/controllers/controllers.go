package controllers

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/mongodbinc-interns/mongoproxy/convert"
	. "github.com/mongodbinc-interns/mongoproxy/log"
	"github.com/mongodbinc-interns/mongoproxy/modules/bi"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"net/http"
	"strconv"
	"time"
)

// biModule is an instance of a BI Module used as reference for the frontend.
var biModule bi.BIModule

// Temporary code to connect to a MongoDB instance. TODO: use the configuration
// from the biModule!
var mongoSession *mgo.Session
var mongoDBDialInfo = &mgo.DialInfo{
	// TODO: Allow configurable connection info
	Addrs:    []string{"localhost:27017"},
	Timeout:  60 * time.Second,
	Database: "test",
}

func init() {
	var err error
	mongoSession, err = mgo.DialWithInfo(mongoDBDialInfo)
	if err != nil {
		Log(ERROR, "%#v\n", err)
		return
	}
}

// metricParam contains the parameters from the URL GET request for metrics.
type metricParam struct {
	// the index for the rule that is referenced by the GET request
	RuleIndex int64

	// the time granularity of the request
	Granularity string

	// the start time queried for in the request
	Start time.Time

	// the end time queried for in the request
	End time.Time
}

func getGranularityField(granularity string) (string, error) {
	switch granularity {
	case bi.Monthly:
		return "month", nil
	case bi.Daily:
		return "day", nil
	case bi.Hourly:
		return "hour", nil
	case bi.Minutely:
		return "minute", nil
	case bi.Secondly:
		return "second", nil
	default:
		return "", fmt.Errorf("Not a valid time granularity %v", granularity)
	}
}

func getRangeInGranularities(startTime time.Time, endTime time.Time, granularity string) (int, error) {
	r := 0

	rDuration := endTime.Sub(startTime)
	switch granularity {
	case bi.Monthly:
		// we assume 30 days in a month for now.
		hours := rDuration / time.Hour
		days := int(hours) / 24
		r = days / 30
	case bi.Daily:
		hours := rDuration / time.Hour
		r = int(hours) / 24
	case bi.Hourly:
		r = convert.ToInt(rDuration.Hours())
	case bi.Minutely:
		r = convert.ToInt(rDuration.Minutes())
	case bi.Secondly:
		r = convert.ToInt(rDuration.Seconds())
	default:
		return 0, fmt.Errorf("Not a valid time granularity %v", granularity)
	}
	return r, nil
}

func addGranularitiesToTime(t time.Time, granularity string, n int) (time.Time, error) {
	num := time.Duration(n)
	switch granularity {
	case bi.Monthly:
		return t.AddDate(0, n, 0), nil
	case bi.Daily:
		return t.AddDate(0, 0, n), nil
	case bi.Hourly:
		return t.Add(time.Hour * num), nil
	case bi.Minutely:
		return t.Add(time.Minute * num), nil
	case bi.Secondly:
		return t.Add(time.Second * num), nil
	default:
		return t, fmt.Errorf("Not a valid time granularity %v", granularity)
	}
}

// parseMetricParams is a helper function to store the URL parameters from a
// request into a metricParam struct.
func parseMetricParams(c *gin.Context) (*metricParam, error) {
	ruleIndex, err := strconv.ParseInt(c.Param("ruleIndex"), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("Invalid ruleIndex: not a number")
	}
	if ruleIndex < 0 || ruleIndex >= int64(len(biModule.Rules)) {
		return nil, fmt.Errorf("Invalid ruleIndex: out of range")
	}

	granularity := c.Param("granularity")

	var start time.Time
	err = start.UnmarshalText([]byte(c.Param("start")))
	if err != nil {
		return nil, fmt.Errorf("Invalid start time: %v", c.Param("start"))
	}

	var end time.Time
	err = end.UnmarshalText([]byte(c.Param("end")))
	if err != nil {
		return nil, fmt.Errorf("Invalid end time: %v", c.Param("end"))
	}

	return &metricParam{
		ruleIndex, granularity, start, end,
	}, nil
}

// getMain is the handler for the main HTML page, and serves up the default view.
func getMain(c *gin.Context) {

	c.HTML(http.StatusOK, "index.html", gin.H{
		"module": biModule,
	})
}

// getMetric is the handler for retrieving data in the form of documents, as they
// are stored in the MongoDB database.
func getMetric(c *gin.Context) {
	params, err := parseMetricParams(c)

	if err != nil {
		c.JSON(400, gin.H{
			"error": err,
		})
		return
	}

	docs, err := getDataOverRange(mongoSession, biModule.Rules[params.RuleIndex],
		params.Granularity, params.Start, params.End)
	if err != nil {
		c.JSON(400, gin.H{
			"error": err,
		})
		return
	}
	c.JSON(200, docs)
}

// getTabularMetric is the handler for retrieving tabular data.
func getTabularMetric(c *gin.Context) {
	params, err := parseMetricParams(c)

	if err != nil {
		c.JSON(400, gin.H{
			"error": err,
		})
		return
	}

	input, err := getDataOverRange(mongoSession, biModule.Rules[params.RuleIndex],
		params.Granularity, params.Start, params.End)
	if err != nil {
		c.JSON(400, gin.H{
			"error": err,
		})
		return
	}

	r, err := getRangeInGranularities(params.Start, params.End, params.Granularity)

	if err != nil {
		c.JSON(400, gin.H{
			"error": err,
		})
		return
	}

	if r > 10000 {
		c.JSON(400, gin.H{
			"error": "Too many results to tabulate",
		})
		return
	}
	timeArray := make([]time.Time, r)
	currentTime := params.Start
	for i := 0; i < r; i++ {
		timeArray[i] = currentTime
		currentTime, _ = addGranularitiesToTime(currentTime, params.Granularity, 1)
	}

	// initialize the data array
	dataArray := make([]float64, r)

	// we have nothing. Return as is.
	if len(input) == 0 {
		c.JSON(200, gin.H{
			"data": dataArray,
			"time": timeArray,
		})
		return
	}

	for i := 0; i < len(input); i++ {
		var ticks int
		inputStartTime, ok := input[i]["start"].(time.Time)
		if !ok {
			continue
		}
		switch params.Granularity {
		case bi.Monthly:
			ticks = 12
		case bi.Daily:
			switch inputStartTime.Month() {
			case time.April:
				fallthrough
			case time.June:
				fallthrough
			case time.September:
				fallthrough
			case time.November:
				ticks = 30
			case time.February:
				ticks = 28 // TODO: account for leap years
			default:
				ticks = 31
			}
		case bi.Hourly:
			ticks = 24
		default:
			ticks = 60
		}

		granularityField, err := getGranularityField(params.Granularity)
		if err != nil {
			c.JSON(400, gin.H{
				"error": "Not a valid time granularity",
			})
			return
		}
		dataField, ok := input[i][granularityField].(bson.M)
		if !ok {
			continue
		}
		for j := 0; j < ticks; j++ {
			val := convert.ToFloat64(dataField[strconv.Itoa(j)], 0)
			cTime, _ := addGranularitiesToTime(inputStartTime, params.Granularity, j)

			index, _ := getRangeInGranularities(params.Start, cTime, params.Granularity)
			// indexes are going to be off by 1, since we want to be inclusive of the end time.
			if index > 0 && index <= r {
				dataArray[index-1] = val
			}
		}
	}

	c.JSON(200, gin.H{
		"data": dataArray,
		"time": timeArray,
	})

}

// Setup sets up the routes for the frontend server, taking in an Engine
// and a BI Module for initialization, and returns the same Engine with the
// routes added for chaining purposes.
func Setup(r *gin.Engine, source bi.BIModule) *gin.Engine {
	biModule = source

	r.GET("/", getMain)
	r.GET("/data/:ruleIndex/:granularity/:start/:end", getMetric)
	r.GET("/tabular/:ruleIndex/:granularity/:start/:end", getTabularMetric)

	r.Static("/public", "./public")
	return r
}