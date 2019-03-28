// Package bsonutil provides a utility function to retrieve values from BSON documents.
package bsonutil

import (
	"strings"

	"github.com/mongodb-labs/mongoproxy/convert"
	"go.mongodb.org/mongo-driver/bson"
)

// FindValueByKey returns the value of keyName in document. If keyName is not found
// in the top-level of the document, a nil document is returned.
func FindValueByKey(keyName string, document bson.D) interface{} {
	for _, kvPair := range document {
		if kvPair.Key == keyName {
			return kvPair.Value
		}
	}
	return nil
}

// FindDeepValueInMap returns the value of a dot-notation-separated keyName in a map m.
// If the path is not found in the document, a nil document is returned.
func FindDeepValueInMap(keyName string, m bson.M) interface{} {
	keyChain := strings.Split(keyName, ".")
	currentDoc := m
	var val interface{}
	for i := 0; i < len(keyChain); i++ {
		val = currentDoc[keyChain[i]]

		if i < len(keyChain)-1 {
			currentDoc = convert.ToBSONMap(val)
			if currentDoc == nil {
				return nil
			}
		}
	}
	return val
}
