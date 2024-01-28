package utils

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetTestKV(t *testing.T) {
	for i := 0; i < 10; i++ {
		assert.NotNil(t, string(GetTestKey(i)))
		t.Log(string(GetTestKey(i)))
	}
}

func TestRandomValue(t *testing.T) {
	for i := 0; i < 10; i++ {
		assert.NotNil(t, RandomValue(10))
		t.Log(string(RandomValue(10)))
	}
}
