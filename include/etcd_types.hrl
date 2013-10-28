-record(error, {
          errorCode :: integer(),
          message :: binary(),
          cause :: binary()
         }).
-record(set, {
          key :: binary(),
          value :: binary(),
          prevValue :: binary(),
          newKey = false :: boolean(),
          expiration :: binary(),
          ttl :: integer(),
          index :: integer()
         }).
-record(get, {
          key :: binary(),
          value :: binary(),
          dir = false :: boolean(),
          index :: integer()
         }).
-record(delete, {
          key :: key(),
          prevValue :: binary(),
          index :: integer()
         }).

-type url() :: string().
-type key() :: string() | binary().
-type value() :: string() | binary().
-type pos_timeout() ::  pos_integer() | 'infinity'.
-type result() :: {ok, response() | [response()]} | {http_error, atom()}.
-type response() :: #error{} | #set{} | #get{} | #delete{}.
