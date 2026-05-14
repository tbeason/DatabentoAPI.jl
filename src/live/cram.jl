const CRAM_BUCKET_ID_LENGTH = 5

"""
    cram_response(challenge, api_key) -> String

Compute the Databento CRAM authentication response.

Format: `hex(sha256("{challenge}|{api_key}")) * "-" * api_key[end-4:end]`.

This mirrors `databento.common.cram.get_challenge_response` in the Python client.
"""
function cram_response(challenge::AbstractString, api_key::AbstractString)::String
    length(api_key) >= CRAM_BUCKET_ID_LENGTH ||
        throw(BentoAuthError("API key must be at least $CRAM_BUCKET_ID_LENGTH characters"))
    digest_hex = bytes2hex(SHA.sha256(string(challenge, "|", api_key)))
    bucket_id = api_key[end-CRAM_BUCKET_ID_LENGTH+1:end]
    return string(digest_hex, "-", bucket_id)
end
