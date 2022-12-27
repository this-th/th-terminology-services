# Input example
# {
#   "token": "<bearer_token>",
#   "method" "GET"
# }
package terminology.snowstorm
import future.keywords.if

default allow := false

allow if {
    jwt_verified
    allow_only_get
}

allow if {
    jwt_verified
    is_admin
}

jwt_payload = payload if {
	[_, payload, _] := io.jwt.decode(input.token)
}

jwt_verified if {
	jwks := json.marshal(data.jwks)
	now := time.now_ns()
	# Check signature
	io.jwt.verify_rs256(input.token, jwks)
	# Check expire time
	jwt_payload.exp * 1000 * 1000 * 1000 > now
	# Check audience
	"https://snowstorm.sil-th.org" == jwt_payload.aud[i]
}

user_roles = jwt_payload.realm_access.roles

allow_only_get {
    "GET" == input.method
    "TerminologyViewer" == user_roles[i]
}

is_admin {
	"TerminologyAdmin" == user_roles[i]
}
