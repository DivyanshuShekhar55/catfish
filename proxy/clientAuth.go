package proxy

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/DivyanshuShekhar55/catfish/config"
	"github.com/jackc/pgx/v5/pgproto3"
	"golang.org/x/crypto/pbkdf2"
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
	case AuthMD5:
		if err := authMD5(backend, entry); err != nil {
			return err
		}
	case AuthSCRAM:
		if err := authSCRAM(backend, entry); err != nil {
			return err
		}
	default:
		return fmt.Errorf(ErrUnknownAuthMethod.Error(), username, database)

	}

	// set following fields in the state now for this user
	clientState.username = username
	clientState.database = database
	clientState.tier = entry.Tier

	// auth succeeded (idk it's succeeded or succeded)
	// buffer the authOk msg
	backend.Send(&pgproto3.AuthenticationOk{})

	// Send a minimal set of ParameterStatus messages — clients expect these.
	for name, val := range s.parameterStatuses {
		backend.Send(&pgproto3.ParameterStatus{
			Name:  name,
			Value: val,
		})
	}
	// flsuh all the statuses + authOK together
	if err := backend.Flush(); err != nil {
		return errors.Join(ErrParameterStatusSend, ErrAuthOKSend, err)
	}

	// send ready for query signal
	// everything done now in this func ;)
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := backend.Flush(); err != nil {
		return errors.Join(ErrReadyForQuerySend, err)
	}

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

// Method 3: SCRAM-SHA-256
// SCRAM (Salted Challenge Response Authentication Mechanism) is the modern Postgres auth method
// It proves the client knows the password WITHOUT sending the password or a hash of it on the wire
// even the server never sees the raw password after setup.
//
// The handshake has 3 round trips:
//
//   Round 1 — Client sends: "n,,n=username,r=clientNonce"
//              (n = username, r = random nonce)
//
//   Round 2 — Server sends: "r=clientNonce+serverNonce,s=salt,i=iterations"
//              Server has extended the nonce and given the client the salt+iterations
//              to derive the keys from the password.
//              Client responds: "c=channelBinding,r=fullNonce,p=clientProof"
//              (p= cryptographic proof client knows the password)
//
//   Round 3 — Server sends: "v=serverSignature"
//              Client verifies server also knows the password (mutual auth).
//              Server sends AuthenticationOK.

const scramIterations = 4096

func authSCRAM(backend *pgproto3.Backend, entry config.User) error {
	// Tell client we want SCRAM-SHA-256.
	backend.Send(&pgproto3.AuthenticationSASL{
		AuthMechanisms: []string{"SCRAM-SHA-256"},
	})
	if err := backend.Flush(); err != nil {
		return fmt.Errorf(ErrSCRAMAuthChallengeSend.Error(), err)
	}

	// Round 1: read client-first-message
	msg, err := backend.Receive()
	if err != nil {
		return fmt.Errorf(ErrSCRAMAuthRead.Error(), err)
	}

	saslInit, ok := msg.(*pgproto3.SASLInitialResponse)
	if !ok {
		return fmt.Errorf(ErrSCRAMAuthUnexpectedFormat.Error(), msg)
	}

	if saslInit.AuthMechanism != "SCRAM-SHA-256" {
		return fmt.Errorf(ErrSCRAMAuthUnexpectedMethod.Error(), saslInit.AuthMechanism)
	}

	// Parse client-first-message: "n,,n=<user>,r=<clientNonce>"
	// The "n,," prefix is the GS2 channel binding header (no binding).
	clientFirst := string(saslInit.Data)
	clientFirstBare, clientNonce, err := parseClientFirst(clientFirst)
	if err != nil {
		return fmt.Errorf("scram: parse client-first: %w", err)
	}

	// Round 2: send server-first-message, read client-final-message

	// Generate server nonce = clientNonce + random suffix.
	serverNonceSuffix := make([]byte, 18)
	if _, err := rand.Read(serverNonceSuffix); err != nil {
		return fmt.Errorf("catfish/proxy : scram nonce generation failed ", err)
	}
	serverNonce := clientNonce + base64.StdEncoding.EncodeToString(serverNonceSuffix)

	// generate suffix
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return fmt.Errorf("catfish/proxy : scram salt generation error ", err)
	}
	saltB64 := base64.StdEncoding.EncodeToString(salt)

	// server-first-message
	serverFirst := fmt.Sprintf("r=%s,s=%s,i=%d", serverNonce, saltB64, scramIterations)

	backend.Send(&pgproto3.AuthenticationSASLContinue{
		Data: []byte(serverFirst),
	})
	if err := backend.Flush(); err != nil {
		return fmt.Errorf("catfish/proxy : scram send server-first ", err)
	}

	// reads client's final msg
	msg, err = backend.Receive()
	if err != nil {
		return fmt.Errorf("catfish/proxy : scram read client-final ", err)
	}

	saslResponse, ok := msg.(*pgproto3.SASLResponse)
	if !ok {
		return fmt.Errorf("catfish/proxy : scram unexpected SASLResponse ", msg)
	}

	// Parse client-final: "c=<binding>,r=<fullNonce>,p=<clientProof>"
	clientFinalWithoutProof, clientProofB64, err := parseClientFinal(string(saslResponse.Data), serverNonce)
	if err != nil {
		return fmt.Errorf("scram: parse client-final: %w", err)
	}

	// Verify client proof
	// TODO :
	// This is the expensive part — in production we should cache SaltedPassword per user.
	saltedPassword := pbkdf2.Key(
		[]byte(entry.Password),
		salt,
		scramIterations,
		32, // SHA-256 output = 32 bytes
		sha256.New,
	)

	clientKey := hmacSHA256(saltedPassword, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)

	// AuthMessage = clientFirstBare + "," + serverFirst + "," + clientFinalWithoutProof
	authMessage := clientFirstBare + "," + serverFirst + "," + clientFinalWithoutProof

	clientSignature := hmacSHA256(storedKey[:], []byte(authMessage))

	// ClientProof = ClientKey XOR ClientSignature
	expectedProof := make([]byte, len(clientKey))
	for i := range clientKey {
		expectedProof[i] = clientKey[i] ^ clientSignature[i]
	}

	clientProof, err := base64.StdEncoding.DecodeString(clientProofB64)
	if err != nil {
		return fmt.Errorf("scram: decode client proof: %w", err)
	}

	if !hmac.Equal(clientProof, expectedProof) {
		return fmt.Errorf("scram: wrong password for user %q", entry.Username)
	}

	// Round 3: send server signature (proves server also knows the password)

	serverKey := hmacSHA256(saltedPassword, []byte("Server Key"))
	serverSignature := hmacSHA256(serverKey, []byte(authMessage))
	serverFinal := "v=" + base64.StdEncoding.EncodeToString(serverSignature)

	backend.Send(&pgproto3.AuthenticationSASLFinal{
		Data: []byte(serverFinal),
	})
	if err := backend.Flush(); err != nil {
		return fmt.Errorf("scram: send server-final: %w", err)
	}

	return nil
}

// parseClientFirst parses "n,,n=username,r=clientNonce"
// Returns (clientFirstBare, clientNonce, error).
// clientFirstBare strips the "n,," GS2 prefix — used in AuthMessage later.
func parseClientFirst(msg string) (string, string, error) {
	// Strip GS2 header "n,,"
	if !strings.HasPrefix(msg, "n,,") {
		return "", "", fmt.Errorf("expected GS2 header 'n,,', got %q", msg[:min(6, len(msg))])
	}
	bare := msg[3:] // "n=username,r=clientNonce"

	var clientNonce string
	for _, part := range strings.Split(bare, ",") {
		if strings.HasPrefix(part, "r=") {
			clientNonce = part[2:]
		}
	}

	if clientNonce == "" {
		return "", "", fmt.Errorf("no client nonce in client-first-message")
	}

	return bare, clientNonce, nil
}

// parseClientFinal parses "c=<binding>,r=<fullNonce>,p=<proof>"
// Verifies the full nonce matches what we sent.
// Returns (clientFinalWithoutProof, clientProofB64, error).
func parseClientFinal(msg, expectedNonce string) (string, string, error) {
	parts := strings.Split(msg, ",")
	if len(parts) < 3 {
		return "", "", fmt.Errorf("malformed client-final-message")
	}

	var proof string
	withoutProof := []string{}

	for _, part := range parts {
		if strings.HasPrefix(part, "p=") {
			proof = part[2:]
		} else {
			withoutProof = append(withoutProof, part)
			if strings.HasPrefix(part, "r=") {
				nonce := part[2:]
				if nonce != expectedNonce {
					return "", "", fmt.Errorf("nonce mismatch: expected %q got %q", expectedNonce, nonce)
				}
			}
		}
	}

	if proof == "" {
		return "", "", fmt.Errorf("no proof in client-final-message")
	}

	return strings.Join(withoutProof, ","), proof, nil
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}
