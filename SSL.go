package db

import (
	"crypto/tls"
	"crypto/x509"
	"github.com/go-sql-driver/mysql"
	log "github.com/ssgo/log"
)

func RegisterSSL(name, ca, cert, key string, insecure bool) {
	caPool := x509.NewCertPool()
	clientCert := make([]tls.Certificate, 0, 1)
	if caPool.AppendCertsFromPEM([]byte(ca)) {
		certs, err := tls.X509KeyPair([]byte(cert), []byte(key))
		if err == nil {
			clientCert = append(clientCert, certs)
			err = mysql.RegisterTLSConfig(name, &tls.Config{
				Certificates:       clientCert,
				RootCAs:            caPool,
				InsecureSkipVerify: insecure,
			})
			if err != nil {
				log.DefaultLogger.Error(err.Error())
			}
		} else {
			log.DefaultLogger.Error(err.Error())
		}
	} else {
		log.DefaultLogger.Error("ca error for db")
	}
}
