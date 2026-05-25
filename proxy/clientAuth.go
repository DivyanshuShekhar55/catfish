package proxy

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"

	"github.com/DivyanshuShekhar55/catfish/config"
	"github.com/jackc/pgx/v5/pgproto3"
)

// AuthMethod controls which challenge pgkeeper sends to connecting apps.
// Set per-user in pgkeeper.yml under auth_method.
//
//	cleartext     — plaintext password, fine on trusted/local networks
//	md5           — MD5 hashed password + salt, pg default for years
//	scram-sha-256 — modern, recommended for any real deployment
type AuthMethod string

const (
	AuthClearText AuthMethod = "cleartext"
	AuthMD5       AuthMethod = "md5"
	AuthSCRAM     AuthMethod = "scram-sha-256"
)

// doAuth handles the full Postgres auth handshake for one connecting app.
// It reads the startup message, checks credentials using the configured method,
// populates clientState, and sends AuthenticationOK + ReadyForQuery on success.
// doAuth relays the entire Postgres auth conversation between app and Postgres.

func (s *CatfishServer) doAuth(backend *pgproto3.Backend, appConn net.Conn, clientState *clientState) error {
	// 1. read startup msg from app
	startupMsg, err := backend.ReceiveStartupMessage()
	if err != nil {
		return fmt.Errorf(ErrReadStartupMsg.Error(), err)
	}

	// Handle SSL req, decline TLS for now
	// TODO : TLS setup as well
	if _, ok := startupMsg.(*pgproto3.SSLRequest); ok {
		appConn.Write([]byte{'N'}) // 'N' = no SSL for now, sends back to client
		// client will send user+db now to backend
		// update startup msg
		startupMsg, err = backend.ReceiveStartupMessage()
		if err != nil {
			return fmt.Errorf(ErrReadStartupMsgAfterSSLDecline.Error(), err)
		}

	}
	sm, ok := startupMsg.(*pgproto3.StartupMessage)
	if !ok {
		return fmt.Errorf(ErrStartupMsgUnexpectedFormat.Error(), startupMsg)
	}

	// 2. Look up user in config
	// can extract these two fields from the returned value after user says "I am X and want to connect to db Y"
	// but was (X, Y) configured at the load time of catfish service?
	username := sm.Parameters["user"]
	database := sm.Parameters["database"]

	// lokup user in config
	entry, wasFound := s.userIndex[username]
	if !wasFound {
		return fmt.Errorf(ErrUnknownUser.Error(), username)
	}

	// user was in entry, ebrything good
	// run the configured auth method
	// check this user is allowed to connect to this db
	if entry.Database != database {
		return fmt.Errorf(ErrDatabaseConnectionNotConfigured.Error(), username, database)
	}

	// 3. Run the authentication method configured for this user
	method := AuthMethod(entry.AuthMethod)
	if method == "" {
		method = AuthSCRAM
	}

	switch method {
	case AuthClearText:
		if err := authClearText(backend, entry); err != nil {
			return err
		}
	}

	// Open raw TCP connection to real postgres now
	// upto now we were handling the messages with the client
	// auth ahead will just be blindly forwarded now
	//pgConn, err := net.Dial("tcp", postgresAddr(s.config.PostgresDSN))
	return nil
}

// auth cleartext
// sends password in plain text.
func authClearText(backend *pgproto3.Backend, entry config.User) error {
	// Tell app: send your password as plain text.
	backend.Send(&pgproto3.AuthenticationCleartextPassword{})
	if err := backend.Flush(); err != nil {
		return fmt.Errorf(ErrClearTextAuthChallengeSend.Error(), err)
	}

	// recv the clear text credentials
	msg, err := backend.Receive()
	if err != nil {
		return fmt.Errorf(ErrClearTextAuthRead.Error(), err)
	}

	// get the password from clear text msg
	pwMsg, ok := msg.(*pgproto3.PasswordMessage)
	if !ok {
		return fmt.Errorf(ErrClearTextAuthUnexpectedFormat.Error(), msg)
	}

	if pwMsg.Password != entry.Password {
		return fmt.Errorf(ErrClearTextAuthInvalidPassword.Error(), entry.Username)
	}

	// auth succeded
	return nil
}

// Method 2: MD5
// Postgres MD5 auth works like this:
//  1. Server sends a 4-byte random salt to the client
//  2. Client computes: md5(md5(password + username) + salt)
//  3. Client sends "md5" + that hex string
//  4. Server does the same computation and compares
//
// It hides the password on the wire but MD5 is weak by modern standards.
// Better than cleartext but SCRAM is preferred for new deployments.
func authMD5(backend *pgproto3.Backend, entry config.User) error {
	// generate random salt
	var salt [4]byte
	if _, err := rand.Read(salt[:]); err != nil {
		return fmt.Errorf(ErrMD5AuthSaltGen.Error(), err)
	}

	// send
	backend.Send(&pgproto3.AuthenticationMD5Password{Salt: salt})
	if err := backend.Flush(); err != nil {
		return fmt.Errorf(ErrMD5AuthChallengeSend.Error(), err)
	}

	// read client's hashed response
	msg, err := backend.Receive()
	if err != nil {
		return fmt.Errorf(ErrMD5AuthRead.Error(), err)
	}

	pwMsg, ok := msg.(*pgproto3.PasswordMessage)
	if !ok {
		return fmt.Errorf(ErrMD5AuthUnexpectedFormat.Error(), msg)
	}

	// compute hash's value, will later be comapred with received hash
	expectedHash := md5Password(entry.Password, entry.Username, salt[:])

	if pwMsg.Password != expectedHash {
		return fmt.Errorf(ErrMD5AuthInvalidCredentials.Error(), entry.Username)
	}

	return nil
}

func md5Password(password, username string, salt []byte) string {
	// Step 1: inner hash = md5(password + username)
	inner := md5.New()
	inner.Write([]byte(password))
	inner.Write([]byte(username))
	innerSum := inner.Sum(nil) // nil will force go to allocate fresh memory allocation

	// Convert inner sum to a 32-character hex string buffer
	innerHex := make([]byte, 32)
	hex.Encode(innerHex, innerSum)

	// Step 2: outer hash = md5(innerHexStr + binarySalt)
	outer := md5.New()
	outer.Write(innerHex)
	outer.Write(salt[:]) // Pass the 4 raw binary bytes
	outerSum := outer.Sum(nil)

	// Encode final result and prefix with "md5"
	finalHex := make([]byte, 32)
	hex.Encode(finalHex, outerSum)

	return "md5" + string(finalHex)
}
