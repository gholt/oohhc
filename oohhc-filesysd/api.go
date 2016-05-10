// Structures used in Group Store
//  File System
//  /acct/(uuid)/fs  "(uuid)"    { "id": "uuid", "name": "name", "status": "active",
//                                "createdate": <timestamp>, "deletedate": <timestamp>
//                               }
//
// IP Address
// /acct/(uuid)/fs/(uuid)/addr "(uuid)"   { "id": uuid, "addr": "111.111.111.111", "status": "active",
//                                         "createdate": <timestamp>, "deletedate": <timestamp>
//                                       }

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/gholt/brimtime"
	"github.com/gholt/store"
	fb "github.com/letterj/oohhc/proto/filesystem"
	"github.com/satori/go.uuid"
	"github.com/spaolacci/murmur3"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
)

var errf = grpc.Errorf

// AcctPayLoad ...
type AcctPayLoad struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Token      string `json:"token"`
	Status     string `json:"status"`
	CreateDate int64  `json:"createdate"`
	DeleteDate int64  `json:"deletedate"`
}

// TokenRef ...
type TokenRef struct {
	TokenID string `json:"tokenid"`
	AcctID  string `json:"acctid"`
}

// FileSysRef ...
type FileSysRef struct {
	FSID   string `json:"fsid"`
	AcctID string `json:"acctid"`
}

// FileSysAttr ...
type FileSysAttr struct {
	Attr  string `json:"attr"`
	Value string `json:"value"`
	FSID  string `json:"fsid"`
}

// AddrRef ...
type AddrRef struct {
	Addr string `json:"addr"`
	FSID string `json:"fsid"`
}

// FileSysMeta ...
type FileSysMeta struct {
	ID     string   `json:"id"`
	AcctID string   `json:"acctid"`
	Name   string   `json:"name"`
	Status string   `json:"status"`
	Addr   []string `json:"addrs"`
}

// FileSystemAPIServer is used to implement oohhc
type FileSystemAPIServer struct {
	gstore store.GroupStore
}

// NewFileSystemAPIServer ...
func NewFileSystemAPIServer(store store.GroupStore) *FileSystemAPIServer {
	s := new(FileSystemAPIServer)
	s.gstore = store
	return s
}

// CreateFS ...
func (s *FileSystemAPIServer) CreateFS(ctx context.Context, r *fb.CreateFSRequest) (*fb.CreateFSResponse, error) {
	srcAddr := ""

	// Get incomming ip
	pr, ok := peer.FromContext(ctx)
	if ok {
		srcAddr = pr.Addr.String()
	}

	// Validate Token
	acctID, err := s.validateToken(r.Token)
	if err != nil {
		log.Printf("%s CREATE FAILED %s", srcAddr, "PermissionDenied")
		return nil, errf(codes.PermissionDenied, "%v", "Invalid Token")
	}

	// Write file system entries.
	// FSID = new uuid
	// write /fs 								FSID						FileSysRef
	// write /acct/acctID				FSID						FileSysRef
	// write /fs/FSID						acctID					FileSysAttr
	// write /fs/FSID						Name						FileSysAttr
	// write /fs/FSID						active					FilesysAttr

	// Return File System UUID
	FSID := uuid.NewV4().String()
	// Log Operation
	log.Printf("%s CREATE SUCCESS %s", srcAddr, FSID)
	return &fb.CreateFSResponse{Data: FSID}, nil
}

// ShowFS ...
func (s *FileSystemAPIServer) ShowFS(ctx context.Context, r *fb.ShowFSRequest) (*fb.ShowFSResponse, error) {
	srcAddr := ""

	// Get incomming ip
	pr, ok := peer.FromContext(ctx)
	if ok {
		srcAddr = pr.Addr.String()
	}

	// Validate Token
	acctID, err := s.validateToken(r.Token)
	if err != nil {
		log.Printf("%s SHOW FAILED %s", srcAddr, "PermissionDenied")
		return nil, errf(codes.PermissionDenied, "%v", "Invalid Token")
	}

	// Read list of granted ip addresses
	// group-lookup printf("/fs/%s/addr", FSID)

	// Read the file system attributes
	// group-lookup /fs			FSID
	//		Iterate over all the atributes

	// Return File System

	// Log Operation
	log.Printf("%s SHOW SUCCESS %s", srcAddr, r.FSid)
	return &fb.ShowFSResponse{Data: "OK"}, nil
}

// ListFS ...
func (s *FileSystemAPIServer) ListFS(ctx context.Context, r *fb.ListFSRequest) (*fb.ListFSResponse, error) {
	srcAddr := ""
	// Get incomming ip
	pr, ok := peer.FromContext(ctx)
	if ok {
		srcAddr = pr.Addr.String()
	}
	// Validate Token
	acctID, err := s.validateToken(r.Token)
	if err != nil {
		log.Printf("%s LIST FAILED %s", srcAddr, "PermissionDenied")
		return nil, errf(codes.PermissionDenied, "%v", "Invalid Token")
	}

	// Read a list of file system UUID for each
	// 		Read list of granted addrs for a file system uuid
	// 		group-lookup printf("/fs/%s/addr", FSID)

	// 		Read the file system attributes
	// 		group-lookup /fs			FSID
	//				Iterate over all the atributes

	// return Array of File Systems converted into json
	// Log Operation
	log.Printf("%s LIST SUCCESS %s", srcAddr, "ACCTID")
	return &fb.ListFSResponse{Data: "OK"}, nil
}

// DeleteFS ...
func (s *FileSystemAPIServer) DeleteFS(ctx context.Context, r *fb.DeleteFSRequest) (*fb.DeleteFSResponse, error) {
	var err error
	srcAddr := ""
	// Get incomming ip
	pr, ok := peer.FromContext(ctx)
	if ok {
		srcAddr = pr.Addr.String()
	}

	// validate Token
	acctID, err := s.validateToken(r.Token)
	if err != nil {
		log.Printf("%s DELETE FAILED %s", srcAddr, "PermissionDenied")
		return nil, errf(codes.PermissionDenied, "%v", "Invalid Token")
	}

	// Prep things to return
	// Log Operation
	log.Printf("%s DELETE NOTIMPLEMENTED %s", srcAddr, r.FSid)
	return &fb.DeleteFSResponse{Data: "Delete Operation not supported at this time"}, nil
}

// UpdateFS ...
func (s *FileSystemAPIServer) UpdateFS(ctx context.Context, r *fb.UpdateFSRequest) (*fb.UpdateFSResponse, error) {
	srcAddr := ""
	// Get incomming ip
	pr, ok := peer.FromContext(ctx)
	if ok {
		srcAddr = pr.Addr.String()
	}

	// validate Token
	acctID, err := s.validateToken(r.Token)
	if err != nil {
		log.Printf("%s UPDATE FAILED %s", srcAddr, "PermissionDenied")
		return nil, errf(codes.PermissionDenied, "%v", "Invalid Token")
	}

	// return message
	// Log Operation
	log.Printf("%s UPDATE NOTIMPLEMENTED %s", srcAddr, r.FSid)
	return &fb.UpdateFSResponse{Data: "UPDATE operation is not supported in EA"}, nil
}

// GrantAddrFS ...
func (s *FileSystemAPIServer) GrantAddrFS(ctx context.Context, r *fb.GrantAddrFSRequest) (*fb.GrantAddrFSResponse, error) {

	var addrData AddrRef
	var addrByte []byte
	srcAddr := ""

	// Get incomming ip
	pr, ok := peer.FromContext(ctx)
	if ok {
		srcAddr = pr.Addr.String()
	}
	// validate token
	acctID, err := s.validateToken(r.Token)
	if err != nil {
		log.Printf("%s GRANT FAILED %s", srcAddr, "PermissionDenied")
		return nil, errf(codes.PermissionDenied, "%v", "Invalid Token")
	}

	// GRANT an file system entry for the addr
	// 		write /fs/FSID/addr			addr						AddrRef

	pKey := fmt.Sprintf("/fs/%s/addr", r.FSid)
	pKeyA, pKeyB := murmur3.Sum128([]byte(pKey))
	cKeyA, cKeyB := murmur3.Sum128([]byte(r.Addr))
	timestampMicro := brimtime.TimeToUnixMicro(time.Now())
	addrData.Addr = r.Addr
	addrData.FSID = r.FSid
	err = json.Marshal(addrByte, &addrData)
	if err != nil {
		log.Printf("%s GRANT FAILED %v\n", srcAddr, err)
		return nil, errf(codes.Internal, "%v", err)
	}
	_, err = s.gstore.Write(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, timestampMicro, addrByte)
	if err != nil {
		log.Printf("%s GRANT FAILED %v\n", srcAddr, err)
		return nil, errf(codes.Internal, "%v", err)
	}

	// return Addr was Granted
	// Log Operation
	log.Printf("%s GRANT SUCCESS %s %s", srcAddr, r.FSid, r.Addr)
	return &fb.GrantAddrFSResponse{Data: r.FSid}, nil
}

// RevokeAddrFS ...
func (s *FileSystemAPIServer) RevokeAddrFS(ctx context.Context, r *fb.RevokeAddrFSRequest) (*fb.RevokeAddrFSResponse, error) {
	srcAddr := ""

	// Get incomming ip
	pr, ok := peer.FromContext(ctx)
	if ok {
		srcAddr = pr.Addr.String()
	}
	// Validate Token
	acctID, err := s.validateToken(r.Token)
	if err != nil {
		log.Printf("%s REVOKE FAILED %s", srcAddr, "PermissionDenied")
		return nil, errf(codes.PermissionDenied, "%v", "Invalid Token")
	}

	// REVOKE an file system entry for the addr
	// 		delete /fs/FSID/addr			addr						AddrRef
	pKey := fmt.Sprintf("/fs/%s/addr", r.FSid)
	pKeyA, pKeyB := murmur3.Sum128([]byte(pKey))
	cKeyA, cKeyB := murmur3.Sum128([]byte(r.Addr))
	timestampMicro := brimtime.TimeToUnixMicro(time.Now())
	_, err = s.gstore.Delete(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, timestampMicro)
	if store.IsNotFound(err) {
		log.Printf("%s REVOKE FAILED %s %s", srcAddr, r.FSid, r.Addr)
		return nil, errf(codes.NotFound, "%v", "Not Found")
	}

	// return Addr was revoked
	// Log Operation
	log.Printf("%s REVOKE SUCCESS %s %s", srcAddr, r.FSid, r.Addr)
	return &fb.RevokeAddrFSResponse{Data: r.FSid}, nil
}

// validateToken ...
func (s *FileSystemAPIServer) validateToken(t string) (string, error) {
	var tData TokenRef
	var aData AcctPayLoad
	var tDataByte []byte
	var aDataByte []byte
	var err error

	// Read Token
	pKeyA, pKeyB := murmur3.Sum128([]byte("/token"))
	cKeyA, cKeyB := murmur3.Sum128([]byte(t))
	_, tDataByte, err = s.gstore.Read(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, nil)
	err = json.Unmarshal(tDataByte, &tData)
	if err != nil {
		log.Printf("TOKEN FAIL %v", err)
		return "", err
	}

	// Read Account
	pKeyA, pKeyB = murmur3.Sum128([]byte("/acct"))
	cKeyA, cKeyB = murmur3.Sum128([]byte(tData.AcctID))
	_, aDataByte, err = s.gstore.Read(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, nil)
	if store.IsNotFound(err) {
		return "", errors.New("Not Found")
	}
	err = json.Unmarshal(aDataByte, &aData)
	if err != nil {
		return "", err
	}

	if tData.TokenID != aData.Token {
		// Log Failed Operation
		log.Printf("TOKEN FAIL %s", t)
		return "", errors.New("Invalid Token")
	}

	// Return Account UUID
	// Log Operation
	log.Printf("TOKEN SUCCESS %s", tData.AcctID)
	return tData.AcctID, nil
}
