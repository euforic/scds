package dataserver

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/euforic/scds/database"
	"github.com/labstack/echo"
)

const VERSION = "0.1.0"

// Server is the base struct for the dataserver
type Server struct {
	db *database.BuntDB
	e  *echo.Echo
}

// New creates a new pointer to a server and initilizes the webserver routes
func New() *Server {
	s := Server{}
	s.e = echo.New()
	s.e.HideBanner = true

	s.e.POST("/db/:COL", s.Write)
	s.e.GET("/db/:COL/:ID", s.Read)
	s.e.PUT("/db/:COL/:ID", s.Update)
	s.e.DELETE("/db/:COL/:ID", s.Delete)
	return &s
}

// Response is used to wrap the http response data
type Response struct {
	Error  string          `json:"error,omitempty"`
	Result json.RawMessage `json:"result"`
}

// Start initilizes the database and starts up the webserver
func (s *Server) Start(dbURL string, serverURL string) {
	s.db, _ = database.NewBuntDB(dbURL)
	s.e.Logger.Fatal(s.e.Start(serverURL))
}

// Write is the http endpoint for writing data to the DB
func (s *Server) Write(c echo.Context) error {
	col := c.Param("COL")
	data, err := ioutil.ReadAll(c.Request().Body)
	if err != nil {
		return err
	}

	id, err := s.db.Write(col, string(data))
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, Response{Result: json.RawMessage(`{"id": "` + id + `"}`)})
}

// Read is the http endpoint for reading data from the DB
func (s *Server) Read(c echo.Context) error {
	col := c.Param("COL")
	id := c.Param("ID")

	res, err := s.db.Read(col, id)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, Response{Result: json.RawMessage(res)})
}

// Update is the http endpoint for updating data in the DB
func (s *Server) Update(c echo.Context) error {
	col := c.Param("COL")
	id := c.Param("ID")
	data, err := ioutil.ReadAll(c.Request().Body)
	if err != nil {
		return err
	}

	err = s.db.Update(col, id, string(data))
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, Response{Result: json.RawMessage(`{"success": true}`)})
}

// Delete is the http endpoint for deleting data from the DB
func (s *Server) Delete(c echo.Context) error {
	col := c.Param("COL")
	id := c.Param("ID")

	err := s.db.Delete(col, id, false)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, Response{Result: json.RawMessage(`{"success": true}`)})
}
