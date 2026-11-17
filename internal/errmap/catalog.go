package errmap

// SQS query-protocol error classification types.
const (
    Sender   = "Sender"
    Receiver = "Receiver"
)

const (
    // DDBShapePrefix prefixes every DynamoDB-defined error shape id rendered
    // in the __type member.
    DDBShapePrefix = "com.amazonaws.dynamodb.v20120810#"

    // DDBCoralPrefix prefixes the coral level DynamoDB errors (serialization,
    // authentication), which are the only ones carrying it on the wire.
    DDBCoralPrefix = "com.amazon.coral.service#"

    // SQSNamespace is the XML namespace of SQS query-protocol error
    // responses.
    SQSNamespace = "http://queue.amazonaws.com/doc/2012-11-05/"
)

// Def is the protocol family aware definition of one AWS error: the exact
// wire name, default message and HTTP status per family. A family exposes a
// Def to its renderer when its status is non-zero; a Name/Shape field
// refines the wire value whenever it differs from Code.
type Def struct {
    Code string

    // S3 (REST XML).
    S3Status int
    S3Name   string
    S3Msg    string

    // DynamoDB (AWS JSON 1.0). Shape is the full __type value.
    DDBStatus int
    DDBShape  string
    DDBMsg    string

    // SQS (Query XML). Type is the <Type> classification.
    SQSStatus int
    SQSName   string
    SQSType   string
    SQSMsg    string
}

// Lookup resolves a canonical code to its definition.
func Lookup(code string) (Def, bool) {
    d, ok := index[code]
    return d, ok
}

// catalog transcribes the canonical AWS error definitions Floci emits for
// the S3, DynamoDB and SQS families; it is the single source of truth for
// family wire names, statuses and default messages. Codes shared by several
// families are written as full rows; family-only codes go through the
// s3d/ddbd/ddbc/sqsd/sqsa helpers below.
var catalog = []Def{
    // -----------------------------------------------------------------
    // Errors shared by more than one protocol family.
    // -----------------------------------------------------------------
    {
        Code:      "AccessDenied",
        S3Status:  403,
        S3Msg:     "Access Denied",
        DDBStatus: 400,
        DDBShape:  DDBShapePrefix + "AccessDeniedException",
        DDBMsg:    "User is not authorized to perform this action.",
        SQSStatus: 403,
        SQSType:   Sender,
        SQSMsg:    "Access to the resource is denied.",
    },
    {
        Code:      "InternalError",
        S3Status:  500,
        S3Msg:     "We encountered an internal error. Please try again.",
        DDBStatus: 500,
        DDBShape:  DDBShapePrefix + "InternalServerError",
        DDBMsg:    "We encountered an internal error. Please try again.",
        SQSStatus: 500,
        SQSType:   Receiver,
        SQSMsg:    "We encountered an internal error. Please try again.",
    },
    {
        Code:      "ServiceUnavailable",
        S3Status:  503,
        S3Msg:     "Service Unavailable. Please try again.",
        DDBStatus: 500,
        DDBShape:  DDBShapePrefix + "ServiceUnavailable",
        DDBMsg:    "Service Unavailable - try again later.",
        SQSStatus: 503,
        SQSType:   Receiver,
        SQSMsg:    "Service Unavailable.",
    },
    {
        Code:      "SignatureDoesNotMatch",
        S3Status:  403,
        S3Msg:     "The request signature we calculated does not match the signature you provided. Check your key and signing method.",
        SQSStatus: 403,
        SQSType:   Sender,
        SQSMsg:    "The request signature we calculated does not match the signature you provided. Check your key and signing method.",
    },
    {
        Code:      "ExpiredToken",
        S3Status:  400,
        S3Msg:     "The provided token has expired.",
        DDBStatus: 400,
        DDBShape:  DDBCoralPrefix + "ExpiredTokenException",
        DDBMsg:    "The security token included in the request is expired.",
    },

    // -----------------------------------------------------------------
    // S3 (REST XML).
    // -----------------------------------------------------------------
    s3d("AccountProblem", 403, "There is a problem with your AWS account that prevents the operation from completing successfully."),
    s3d("AllAccessDisabled", 403, "All access to this Amazon S3 bucket has been disabled."),
    s3d("AuthorizationHeaderMalformed", 400, "The authorization header is malformed; check your request."),
    s3d("BadDigest", 400, "The Content-MD5 you specified did not match what we received."),
    s3d("BucketAlreadyExists", 409, "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again."),
    s3d("BucketAlreadyOwnedByYou", 409, "Your previous request to create the named bucket succeeded and you already own it."),
    s3d("BucketNotEmpty", 409, "The bucket you tried to delete is not empty."),
    s3d("EntityTooLarge", 400, "Your proposed upload exceeds the maximum allowed object size."),
    s3d("EntityTooSmall", 400, "Your proposed upload is smaller than the minimum allowed object size."),
    s3d("IncompleteBody", 400, "You did not provide the number of bytes specified by the Content-Length HTTP header."),
    s3d("InvalidAccessKeyId", 403, "The AWS Access Key Id you provided does not exist in our records."),
    s3d("InvalidArgument", 400, "Invalid Argument"),
    s3d("InvalidBucketName", 400, "The specified bucket is not valid."),
    s3d("InvalidBucketState", 409, "The request is not valid with the current state of the bucket."),
    s3d("InvalidDigest", 400, "The Content-MD5 or checksum value that you specified is not valid."),
    s3d("InvalidLocationConstraint", 400, "The specified location-constraint is not valid"),
    s3d("InvalidObjectState", 403, "The operation is not valid for the object's storage class."),
    s3d("InvalidPart", 400, "One or more of the specified parts could not be found. The part may not have been uploaded, or the specified entity tag may not have matched the part's entity tag."),
    s3d("InvalidPartOrder", 400, "The list of parts was not in ascending order. Parts must be ordered by part number."),
    s3d("InvalidRange", 416, "The requested range is not satisfiable"),
    s3d("InvalidRequest", 400, "Invalid Request"),
    s3d("InvalidSecurity", 403, "The provided security credentials are not valid."),
    s3d("InvalidToken", 400, "The provided token is malformed or otherwise invalid."),
    s3d("InvalidURI", 400, "Couldn't parse the specified URI."),
    s3d("KeyTooLongError", 400, "Your key is too long."),
    s3d("MalformedACLError", 400, "The XML you provided was not well-formed or did not validate against our published schema"),
    s3d("MalformedXML", 400, "The XML you provided was not well-formed or did not validate against our published schema"),
    s3d("MaxMessageLengthExceeded", 400, "Your request was too big."),
    s3d("MetadataTooLarge", 400, "Your metadata headers exceed the maximum allowed metadata size."),
    s3d("MethodNotAllowed", 405, "The specified method is not allowed against this resource."),
    s3d("MissingContentLength", 411, "You must provide the Content-Length HTTP header."),
    s3d("MissingRequestBodyError", 400, "Request body is empty."),
    s3d("MissingSecurityHeader", 400, "Your request was missing a required header."),
    s3d("NoSuchBucket", 404, "The specified bucket does not exist"),
    s3d("NoSuchBucketPolicy", 404, "The bucket policy does not exist"),
    s3d("NoSuchCORSConfiguration", 404, "The CORS configuration does not exist"),
    s3d("NoSuchKey", 404, "The specified key does not exist."),
    s3d("NoSuchLifecycleConfiguration", 404, "The lifecycle configuration does not exist"),
    s3d("NoSuchTagSet", 404, "The TagSet does not exist"),
    s3d("NoSuchUpload", 404, "The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed."),
    s3d("NoSuchVersion", 404, "The specified version does not exist."),
    s3d("NotImplemented", 501, "A header you provided implies functionality that is not implemented."),
    s3d("ObjectNotInActiveTierError", 403, "The source object of the COPY operation is not in the active tier and is only stored in archive tier."),
    s3d("OperationAborted", 409, "A conflicting conditional operation is currently in progress against this resource. Please try again."),
    s3d("PreconditionFailed", 412, "At least one of the pre-conditions you specified did not hold"),
    s3d("RequestTimeTooSkewed", 403, "The difference between the request time and the current time is too large."),
    s3d("RequestTimeout", 400, "Your socket connection to the server was not read from or written to within the timeout period."),
    s3d("SlowDown", 503, "Please reduce your request rate."),
    s3d("TooManyBuckets", 400, "You have attempted to create more buckets than allowed."),
    s3d("UnexpectedContent", 400, "This request does not support content"),
    s3d("UnresolvableGrantByEmailAddress", 400, "The email address you provided does not match any account on record."),

    // -----------------------------------------------------------------
    // DynamoDB (AWS JSON 1.0). DynamoDB-defined shapes carry the
    // com.amazonaws.dynamodb.v20120810# prefix; coral level shapes carry
    // com.amazon.coral.service#.
    // -----------------------------------------------------------------
    ddbd("BackupInUseException", 400, "Backup is being used."),
    ddbd("BackupNotFoundException", 400, "Backup not found."),
    ddbd("ConditionalCheckFailedException", 400, "The conditional request failed"),
    ddbd("ContinuousBackupsUnavailableException", 400, "Continuous backups are not enabled for this table."),
    ddbd("GlobalTableAlreadyExistsException", 400, "Global table already exists."),
    ddbd("GlobalTableNotFoundException", 400, "Global table not found."),
    ddbd("IdempotentParameterMismatchException", 400, "Provided idempotency token does not match the previous request's parameters."),
    ddbd("InvalidEndpointException", 400, "The provided endpoint is not valid."),
    ddbd("ItemCollectionSizeLimitExceededException", 400, "The item collection size limit was exceeded."),
    ddbd("LimitExceededException", 400, "Limit for this resource has been exceeded."),
    ddbd("PointInTimeRecoveryUnavailableException", 400, "Point in time recovery is not enabled for this table."),
    ddbd("ProvisionedThroughputExceededException", 400, "The level of provisioned throughput defined for the table is too low. Consider increasing your provisioning level with the UpdateTable API."),
    ddbd("ReplicaAlreadyExistsException", 400, "A replica for this table already exists in the specified region."),
    ddbd("ReplicaNotFoundException", 400, "A replica for this table does not exist in the specified region."),
    ddbd("RequestLimitExceeded", 400, "Too many operations for a given subscriber. Take care not to exceed 4,000 operations per second per table or table index."),
    ddbd("ResourceInUseException", 400, "The resource is already in use."),
    ddbd("ResourceNotFoundException", 400, "Requested resource not found."),
    ddbd("RestoreInProgressException", 400, "The specified table is currently being restored."),
    ddbd("TableAlreadyExistsException", 400, "Table already exists."),
    ddbd("TableInUseException", 400, "Table is being created or updated."),
    ddbd("TableNotFoundException", 400, "Table not found."),
    ddbd("ThrottlingException", 400, "Rate exceeded."),
    ddbd("TransactionCanceledException", 400, "Transaction cancelled, please refer cancellation reasons for specific reasons."),
    ddbd("TransactionInProgressException", 400, "A transaction with the given ID is already in progress."),
    ddbd("ValidationException", 400, "One or more parameter values were invalid."),
    // Coral level shapes (serialization / authentication), which really do
    // carry the com.amazon.coral.service# prefix on the wire. Serialization
    // errors have no message member.
    ddbc("SerializationException", 400, ""),
    ddbc("UnrecognizedClientException", 400, "The security token included in the request is invalid."),

    // -----------------------------------------------------------------
    // SQS (Query XML).
    // -----------------------------------------------------------------
    sqsd("BatchEntryIdsNotDistinct", 400, Sender, "Two or more batch entries in the request have the same Id."),
    sqsd("BatchRequestTooLong", 400, Sender, "The batch request is longer than 262144 bytes."),
    sqsd("EmptyBatchRequest", 400, Sender, "There should be at least one message in the request."),
    sqsd("InvalidAddress", 404, Sender, "The address is not valid for this endpoint."),
    sqsd("InvalidAttributeName", 400, Sender, "The attribute name is invalid."),
    sqsd("InvalidAttributeValue", 400, Sender, "Invalid value for the parameter."),
    sqsd("InvalidBatchEntryId", 400, Sender, "A batch entry id can only contain alphanumeric characters, hyphens and underscores. It can be at most 80 letters long."),
    sqsd("InvalidIdFormat", 400, Sender, "The receipt handle isn't valid for the current version of the API."),
    sqsd("InvalidMessageContents", 400, Sender, "The message contains characters outside the allowed set."),
    sqsd("MessageTooLong", 400, Sender, "Message must be shorter than 262144 bytes."),
    sqsd("MissingParameter", 400, Sender, "The request must contain a parameter."),
    sqsd("OverLimitException", 403, Sender, "The action that you requested would violate a limit (e.g. too many queues)."),
    sqsd("PurgeQueueInProgress", 403, Sender, "PurgeQueue is already in progress on this queue."),
    sqsd("QueueDeletedRecently", 400, Sender, "You must wait 60 seconds after deleting a queue before you can create another one with the same name."),
    // The Query API keeps emitting the legacy alias on the wire.
    sqsa("QueueDoesNotExist", "AWS.SimpleQueueService.NonExistentQueue", 400, Sender, "The specified queue doesn't exist."),
    sqsd("ReceiptHandleIsInvalid", 404, Sender, "The input receipt handle is invalid."),
    sqsd("RequestThrottled", 403, Sender, "Rate exceeded."),
    sqsd("TooManyEntriesInBatchRequest", 400, Sender, "Maximum number of entries per request are 10."),
    sqsd("UnsupportedOperation", 400, Sender, "The specified action is not supported."),
    // Server side KMS errors surfaced through SQS; the wire codes keep the
    // KMS. prefix.
    sqsd("KMS.AccessDeniedException", 403, Sender, "The ciphertext references a key that either doesn't exist or that you don't have access to."),
    sqsd("KMS.DisabledException", 400, Sender, "The ciphertext references a key that is disabled."),
    sqsd("KMS.InvalidStateException", 400, Sender, "The ciphertext references a key that is not in a valid state."),
    sqsd("KMS.NotFoundException", 400, Sender, "The ciphertext references a key that doesn't exist."),
    sqsd("KMS.OptInRequired", 400, Sender, "The AWS Access Key Id needs a subscription for the service."),
    sqsd("KMS.ThrottlingException", 400, Sender, "Rate exceeded."),
}

var index = buildIndex(catalog)

func buildIndex(defs []Def) map[string]Def {
    m := make(map[string]Def, len(defs))
    for _, d := range defs {
        if _, dup := m[d.Code]; dup {
            panic("errmap: duplicate catalog code " + d.Code)
        }
        m[d.Code] = d
    }
    return m
}

func s3d(code string, status int, msg string) Def {
    return Def{Code: code, S3Status: status, S3Msg: msg}
}

func ddbd(code string, status int, msg string) Def {
    return Def{Code: code, DDBStatus: status, DDBShape: DDBShapePrefix + code, DDBMsg: msg}
}

func ddbc(code string, status int, msg string) Def {
    return Def{Code: code, DDBStatus: status, DDBShape: DDBCoralPrefix + code, DDBMsg: msg}
}

func sqsd(code string, status int, sqsType, msg string) Def {
    return Def{Code: code, SQSStatus: status, SQSType: sqsType, SQSMsg: msg}
}

func sqsa(code, wireName string, status int, sqsType, msg string) Def {
    return Def{Code: code, SQSStatus: status, SQSName: wireName, SQSType: sqsType, SQSMsg: msg}
}
