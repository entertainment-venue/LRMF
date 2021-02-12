package lrmf

import "github.com/pkg/errors"

var (
	// exported
	ErrParam = errors.New("param err")

	// internal
	errNodeExist             = errors.New("node exist")
	errNotNodeExist          = errors.New("node not exist")
	errNodeValueAlreadyExist = errors.New("node value already exist")
	errNodeValueErr          = errors.New("node value not equal")
)
