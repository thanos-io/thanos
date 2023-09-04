// Manual code for logging field extraction tests.

package testpb

const TestServiceFullName = "grpc_middleware.testpb.TestService"

// This is implementing tags.requestFieldsExtractor.
func (m *PingRequest) ExtractRequestFields(appendToMap map[string]string) {
	appendToMap["value"] = m.Value
}
