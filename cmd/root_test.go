package cmd

import "testing"

func TestValidatePath(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"", "/"},
		{"/", "/"},
		{"test", "/test"},
		{"/test", "/test"},
		{"test/", "/test"},
		{"/test/", "/test"},
		{"path/to/file", "/path/to/file"},
		{"/path/to/file/", "/path/to/file"},
	}

	for _, test := range tests {
		output, err := validatePath(test.input)
		if err != nil {
			t.Errorf("validatePath(%q) returned error: %v", test.input, err)
		}
		if output != test.expected {
			t.Errorf("validatePath(%q) = %q, want %q", test.input, output, test.expected)
		}
	}
}
