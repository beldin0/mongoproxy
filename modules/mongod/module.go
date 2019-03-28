// Package mongod contains a module that acts as a backend for Mongo proxy,
// which connects to a mongod instance and sends requests to (and receives responses from)
// the server.
package mongod

import (
	"context"
	"fmt"
	"time"

	"github.com/mongodb-labs/mongoproxy/bsonutil"
	"github.com/mongodb-labs/mongoproxy/convert"
	. "github.com/mongodb-labs/mongoproxy/log"
	"github.com/mongodb-labs/mongoproxy/messages"
	"github.com/mongodb-labs/mongoproxy/server"
	"go.mongodb.org/mongo-driver/bson"
	mgo "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// A MongodModule takes the request, sends it to a mongod instance, and then
// writes the response from mongod into the ResponseWriter before calling
// the next module. It passes on requests unchanged.
type MongodModule struct {
	Context       context.Context
	ClientOptions *options.ClientOptions
	Client        *mgo.Client
}

func init() {
	server.Publish(&MongodModule{})
}

// New is the factory function to return a new instance
func (m *MongodModule) New() server.Module {
	return &MongodModule{}
}

// Name returns the name/type of the module
func (m *MongodModule) Name() string {
	return "mongod"
}

type commandBundle struct {
	req     messages.Requester
	res     messages.Responder
	next    server.PipelineFunc
	session *mgo.Client
	context context.Context
	process func()
}

func (cb *commandBundle) Next() {
	cb.next(cb.req, cb.res)
}

func (cb *commandBundle) Run() {
	cb.setupProcessFunction()
	defer func() {
		if r := recover(); r != nil {
			Log(WARNING, "Had to recover: %v", r)
		}
	}()
	cb.process()
}

/*
Configuration structure:
{
	addresses: []string,
	direct: boolean,
	timeout: integer,
	auth: {
		username: string,
		password: string,
		database: string
	}
}
*/

// Configure configures the module
func (m *MongodModule) Configure(conf bson.M) error {
	addrs, ok := conf["addresses"].([]string)
	if !ok {
		// check if it's a slice of interfaces
		addrsRaw, ok := conf["addresses"].([]interface{})
		if !ok {
			return fmt.Errorf("Invalid addresses: not a slice")
		}
		addrs = make([]string, len(addrsRaw))
		for i := 0; i < len(addrsRaw); i++ {
			a, ok := addrsRaw[i].(string)
			if !ok {
				return fmt.Errorf("Invalid addresses: address is not a string")
			}
			addrs[i] = a
		}
	}

	timeout := time.Duration(convert.ToInt64(conf["timeout"], -1))
	if timeout == -1 {
		timeout = time.Second * 10
	}

	m.Context, _ = context.WithTimeout(context.Background(), timeout)

	// dialInfo := mgo.DialInfo{
	// 	Addrs:   addrs,
	// 	Direct:  convert.ToBool(conf["direct"]),
	// 	Timeout: timeout,
	// }

	var username, password string
	auth := convert.ToBSONMap(conf["auth"])
	if auth != nil {
		username, ok = auth["username"].(string)
		if ok {
			// dialInfo.Username = username
		}
		password, ok = auth["password"].(string)
		if ok {
			// dialInfo.Password = password
		}
		// database, ok := auth["database"].(string)
		// if ok {
		// dialInfo.Database = database
		// }
	}

	m.ClientOptions = &options.ClientOptions{
		Hosts: addrs,
		Auth: func() *options.Credential {
			if auth == nil {
				return nil
			}
			return &options.Credential{
				Username: username,
				Password: password,
			}
		}(),
	}
	// m.ClientOptions = dialInfo
	return nil
}

// Process is the function to run the code inside the module
func (m *MongodModule) Process(req messages.Requester, res messages.Responder,
	next server.PipelineFunc) {

	cb := &commandBundle{
		session: m.Client,
		context: m.Context,
		req:     req,
		res:     res,
		next:    next,
	}

	// spin up the cb.session if it doesn't exist
	if m.Client == nil {
		var err error
		m.Client, err = mgo.Connect(m.Context, m.ClientOptions)
		if err != nil {
			Log(ERROR, "Error connecting to MongoDB: %#v", err)
			cb.Next()
			return
		}
		// m.Client.SetPrefetch(0)
	}
	cb.Run()
}

func (cb *commandBundle) setupProcessFunction() {
	switch cb.req.Type() {
	case messages.CommandType:
		cb.process = func() {
			command, err := messages.ToCommandRequest(cb.req)
			if err != nil {
				Log(WARNING, "Error converting to command: %#v", err)
				cb.Next()
				return
			}

			b := command.ToBSON()

			cmdResponse := cb.session.Database(command.Database).RunCommand(cb.context, b)
			if cmdResponse != nil {
				// log an error if we can
				Log(WARNING, "Error running command %v: %v", command.CommandName, err)
				cb.res.Error(-1, "Unknown error")
				cb.Next()
				return
			}

			bytes, err := cmdResponse.DecodeBytes()
			if err != nil {
				Log(WARNING, "Error decoding command response")
				cb.Next()
				return
			}
			var reply bson.M
			err = bson.Unmarshal(bytes, &reply)
			if err != nil {
				Log(WARNING, "Error decoding command response")
				cb.Next()
				return
			}
			response := messages.CommandResponse{
				Reply: reply,
			}

			// if convert.ToInt(reply["ok"]) == 0 {
			// 	// we have a command error.
			// 	cb.res.Error(convert.ToInt32(reply["code"]), convert.ToString(reply["errmsg"]))
			// 	cb.Next()
			// 	return
			// }

			cb.res.Write(response)
			cb.Next()
		}
	case messages.FindType:
		cb.process = func() {
			f, err := messages.ToFindRequest(cb.req)
			if err != nil {
				Log(WARNING, "Error converting to a Find command: %#v", err)
				cb.Next()
				return
			}

			coll := cb.session.Database(f.Database).Collection(f.Collection)
			query, err := coll.Find(cb.context, f.Filter) //.Batch(int(f.Limit)).Skip(int(f.Skip)).Prefetch(0)
			if err != nil {
				Log(WARNING, "Query error")
				cb.Next()
				return
			}

			// if f.Projection != nil {
			// 	query = query.Select(f.Projection)
			// }

			// var iter = query.Iter()
			var results []bson.D

			cursorID := int64(0)

			if f.Limit > 0 {
				// only store the amount specified by the limit
				for i := 0; i < int(f.Limit); i++ {
					ok := query.Next(cb.context)
					if !ok {
						break
					}
					var result bson.D
					query.Decode(result)
					results = append(results, result)
				}
			}

			response := messages.FindResponse{
				Database:   f.Database,
				Collection: f.Collection,
				Documents:  results,
				CursorID:   cursorID,
			}

			cb.res.Write(response)
			cb.Next()
		}
	case messages.InsertType:
		cb.process = func() {
			insert, err := messages.ToInsertRequest(cb.req)
			if err != nil {
				Log(WARNING, "Error converting to Insert command: %#v", err)
				cb.Next()
				return
			}

			b := insert.ToBSON()

			// var docs []interface{}
			// for _, doc := range insert.Documents {
			// 	docs := append(docs, doc)
			// }

			reply := cb.session.Database(insert.Database).RunCommand(cb.context, b)
			if reply.Err() != nil {
				// log an error if we can
				// qErr, ok := err.(*mgo.QueryError)
				// if ok {
				// 	cb.res.Error(int32(qErr.Code), qErr.Message)
				// }
				Log(WARNING, "Failed to insert documents: %s", err)
				cb.Next()
				return
			}

			var doc bson.M
			err = reply.Decode(&doc)
			response := messages.InsertResponse{
				// default to -1 if n doesn't exist to hide the field on export
				N: int32(doc["id"].(int32)),
			}
			// writeErrors, err := convert.ConvertToBSONMapSlice(reply["writeErrors"])
			// if err == nil {
			// 	// we have write errors
			// 	response.WriteErrors = writeErrors
			// }

			// if convert.ToInt(reply["ok"]) == 0 {
			// 	// we have a command error.
			// 	cb.res.Error(convert.ToInt32(reply["code"]), convert.ToString(reply["errmsg"]))
			// 	cb.Next()
			// 	return
			// }

			cb.res.Write(response)
			cb.Next()
		}
	case messages.UpdateType:
		cb.process = func() {
			u, err := messages.ToUpdateRequest(cb.req)
			if err != nil {
				Log(WARNING, "Error converting to Update command: %v", err)
				cb.Next()
				return
			}

			b := u.ToBSON()

			reply := bson.D{}
			cmdResponse := cb.session.Database(u.Database).RunCommand(cb.context, b)
			if cmdResponse.Err() != nil {
				// log an error if we can
				// qErr, ok := err.(*mgo.QueryError)
				// if ok {
				// 	cb.res.Error(int32(qErr.Code), qErr.Message)
				// }
				cb.Next()
				return
			}

			response := messages.UpdateResponse{
				N:         convert.ToInt32(bsonutil.FindValueByKey("n", reply), -1),
				NModified: convert.ToInt32(bsonutil.FindValueByKey("nModified", reply), -1),
			}

			writeErrors, err := convert.ConvertToBSONMapSlice(
				bsonutil.FindValueByKey("writeErrors", reply))
			if err == nil {
				// we have write errors
				response.WriteErrors = writeErrors
			}

			rawUpserted := bsonutil.FindValueByKey("upserted", reply)
			upserted, err := convert.ConvertToBSONDocSlice(rawUpserted)
			if err == nil {
				// we have upserts
				response.Upserted = upserted
			}

			if convert.ToInt(bsonutil.FindValueByKey("ok", reply)) == 0 {
				// we have a command error.
				cb.res.Error(convert.ToInt32(bsonutil.FindValueByKey("code", reply)),
					convert.ToString(bsonutil.FindValueByKey("errmsg", reply)))
				cb.Next()
				return
			}

			cb.res.Write(response)
			cb.Next()
		}
	case messages.DeleteType:
		cb.process = func() {
			d, err := messages.ToDeleteRequest(cb.req)
			if err != nil {
				Log(WARNING, "Error converting to Delete command: %v", err)
				cb.Next()
				return
			}

			b := d.ToBSON()

			reply := cb.session.Database(d.Database).RunCommand(cb.context, b)
			if reply.Err() != nil {
				// log an error if we can
				// qErr, ok := err.(*mgo.QueryError)
				// if ok {
				// 	cb.res.Error(int32(qErr.Code), qErr.Message)
				// }
				cb.Next()
				return
			}

			response := messages.DeleteResponse{
				N: -1,
			}
			// writeErrors, err := convert.ConvertToBSONMapSlice(reply["writeErrors"])
			// if err == nil {
			// 	// we have write errors
			// 	response.WriteErrors = writeErrors
			// }

			// if convert.ToInt(reply["ok"]) == 0 {
			// 	// we have a command error.
			// 	cb.res.Error(convert.ToInt32(reply["code"]), convert.ToString(reply["errmsg"]))
			// 	cb.Next()
			// 	return
			// }

			Log(INFO, "Reply: %#v", reply)

			cb.res.Write(response)
			cb.Next()
		}
	case messages.GetMoreType:
		cb.process = func() {
			// g, err := messages.ToGetMoreRequest(cb.req)
			// if err != nil {
			// 	Log(WARNING, "Error converting to GetMore command: %#v", err)
			// 	cb.Next()
			// 	return
			// }
			// Log(DEBUG, "%#v", g)

			// // make an iterable to get more
			// cb := cb.session.Database(g.Database).C(g.Collection)
			// batch := make([]bson.Raw, 0)
			// iter := cb.NewIter(cb.session, batch, g.CursorID, nil)
			// iter.SetBatch(int(g.BatchSize))

			// var results []bson.D
			// cursorID := int64(0)

			// for i := 0; i < int(g.BatchSize); i++ {
			// 	var result bson.D
			// 	ok := iter.Next(&result)
			// 	if !ok {
			// 		err = iter.Err()
			// 		if err != nil {
			// 			Log(WARNING, "Error on GetMore Command: %#v", err)

			// 			if err == mgo.ErrCursor {
			// 				// we return an empty getMore with an errored out
			// 				// cursor
			// 				response := messages.GetMoreResponse{
			// 					CursorID:      cursorID,
			// 					Database:      g.Database,
			// 					Collection:    g.Collection,
			// 					InvalidCursor: true,
			// 				}
			// 				cb.res.Write(response)
			// 				cb.Next()
			// 				return
			// 			}

			// 			// log an error if we can
			// 			qErr, ok := err.(*mgo.QueryError)
			// 			if ok {
			// 				cb.res.Error(int32(qErr.Code), qErr.Message)
			// 			}
			// 			iter.Close()
			// 			cb.Next()
			// 			return
			// 		}
			// 		break
			// 	}
			// 	if cursorID == 0 {
			// 		cursorID = iter.CursorID()
			// 	}
			// 	results = append(results, result)
			// }

			// response := messages.GetMoreResponse{
			// 	CursorID:   cursorID,
			// 	Database:   g.Database,
			// 	Collection: g.Collection,
			// 	Documents:  results,
			// }

			// cb.res.Write(response)
			cb.Next()
		}
	default:
		cb.process = func() {
			Log(WARNING, "Unsupported operation: %v", cb.req.Type())
			cb.Next()
		}
	}
}
