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

jwt_verified {
    jwks := json.marshal(data.jwks)
	io.jwt.verify_rs256(input.token, jwks)
}

user_roles = payload.realm_access.roles {
	[_, payload, _] := io.jwt.decode(input.token)
}

allow_only_get {
    "GET" == input.method
    "TerminologyViewer" == user_roles[i]
}


is_admin {
	"TerminologyAdmin" == user_roles[i]
}
