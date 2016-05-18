package main

// based on the input of a company name this will output
// the string to create an account using oort-cli

// acctdv2 -N <Account Name> -I [account uuid]

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/codegangsta/cli"
	"github.com/satori/go.uuid"
)

// EA account structure
type acctStruct struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Token      string `json:"token"`
	Status     string `json:"status"`
	CreateDate int64  `json:"createdate"`
	DeleteDate int64  `json:"deletedate"`
}

// EA token structure
type tokenStruct struct {
	Token     string `json:"token"`
	AccountID string `json:"acctid"`
}

func main() {

	var acctName string
	var acctID string
	var acctData acctStruct
	var tokenData tokenStruct

	app := cli.NewApp()
	app.Name = "acctdv2"
	app.Usage = "Tool used to create Account entries"
	app.Version = "0.0.1"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "N",
			Value:       "",
			Usage:       "Account Name",
			Destination: &acctName,
		},
		cli.StringFlag{
			Name:  "I",
			Value: "",
			Usage: "Account UUID used to rewrite account data",
		},
	}
	app.Action = func(c *cli.Context) error {
		// name is a required field
		if acctName == "" {
			fmt.Println("Account Name is a required field.")
			os.Exit(1)
		}
		acctData.Name = acctName
		// if acctID passed in this is a rewrite
		if acctID == "" {
			acctData.ID = uuid.NewV4().String()
		} else {
			idUUID, err := uuid.FromString(acctID)
			if err != nil {
				fmt.Printf("Invalid Account uuid: %v", err)
				os.Exit(1)
			}
			acctData.ID = idUUID.String()
		}
		acctData.Token = uuid.NewV4().String()
		acctData.CreateDate = time.Now().Unix()
		acctData.Status = "active"
		acctData.DeleteDate = 0

		tokenData.AccountID = acctData.ID
		tokenData.Token = acctData.Token

		// Marshal structs into strings
		acctStr, err := json.Marshal(acctData)
		if err != nil {
			fmt.Printf("json error: %v", err)
			os.Exit(1)
		}
		tokenStr, err := json.Marshal(tokenData)
		if err != nil {
			fmt.Printf("json error: %v", err)
			os.Exit(1)
		}
		// write out token group store values
		fmt.Printf("write %s %s %s\n\n", "/token", acctData.Token, tokenStr)
		// wrtie out the account group store values
		fmt.Printf("write %s %s %s\n\n", "/acct", acctData.ID, acctStr)

		return nil
	}
	app.Run(os.Args)
}
