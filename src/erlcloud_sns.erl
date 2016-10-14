%%% @doc Amazon SNS.
%%%      Events are parsed according to http://docs.aws.amazon.com/sns/latest/dg/SendMessageToHttp.html
%%% @todo add all the missing functions for the different actions
-module(erlcloud_sns).
-author('elbrujohalcon@inaka.net').

-export([add_permission/3, add_permission/4,
         create_platform_endpoint/2, create_platform_endpoint/3,
         create_platform_endpoint/4, create_platform_endpoint/5,
         create_platform_endpoint/6,
         create_topic/1, create_topic/2,
         delete_endpoint/1, delete_endpoint/2, delete_endpoint/3,
         delete_topic/1, delete_topic/2,
         list_topics/0, list_topics/1, list_topics/2,
         list_topics_all/0, list_topics_all/1,
         list_endpoints_by_platform_application/1,
         list_endpoints_by_platform_application/2,
         list_endpoints_by_platform_application/3,
         list_endpoints_by_platform_application/4,
         get_endpoint_attributes/1,
         get_endpoint_attributes/2,
         get_endpoint_attributes/3,
         set_endpoint_attributes/2,
         set_endpoint_attributes/3,
         publish_to_topic/2, publish_to_topic/3, publish_to_topic/4,
         publish_to_topic/5, publish_to_target/2, publish_to_target/3,
         publish_to_target/4, publish_to_target/5, publish/5,
         list_platform_applications/0, list_platform_applications/1,
         list_platform_applications/2, list_platform_applications/3,
         confirm_subscription/1, confirm_subscription/2, confirm_subscription/3,
         confirm_subscription2/2, confirm_subscription2/3, confirm_subscription2/4,
         set_topic_attributes/3, set_topic_attributes/4,
         subscribe/3, subscribe/4,
	 unsubscribe/1, unsubscribe/2
         ]).
-export([parse_event/1, get_event_type/1, parse_event_message/1,
         get_notification_attribute/2]).
-export([new/2, new/3, configure/2, configure/3]).

-include("erlcloud.hrl").
-include("erlcloud_aws.hrl").
-define(API_VERSION, "2010-03-31").

-opaque sns_event() :: jsx:json_term().
-opaque sns_notification() :: sns_message().
-type sns_event_type() :: subscription_confirmation | notification.
-export_type([sns_event/0, sns_event_type/0, sns_notification/0]).

-type sns_endpoint_attribute() :: custom_user_data
                                | enabled
                                | token.

-type sns_endpoint() :: [{arn, string()} | {attributes, [{arn|sns_endpoint_attribute(), string()}]}].

-type sns_permission() :: all
                        | add_permission
                        | confirm_subscription
                        | create_platform_application
                        | create_platform_endpoint
                        | create_topic
                        | delete_endpoint
                        | delete_platform_application
                        | delete_topic
                        | get_endpoint_attributes
                        | get_platform_application_attributes
                        | get_subscription_attributes
                        | get_topic_attributes
                        | list_endpoints_by_platform_application
                        | list_platform_applications
                        | list_subscriptions
                        | list_subscriptions_by_topic
                        | list_topics
                        | publish
                        | remove_permission
                        | set_endpoint_attributes
                        | set_platform_application_attributes
                        | set_subscription_attributes
                        | set_topic_attributes
                        | subscribe
                        | unsubscribe.
-type sns_acl() :: [{string(), sns_permission()}].

-type sns_message() :: string() | jsx:json_term().

-type sns_application_attribute() :: event_endpoint_created
                                   | event_endpoint_deleted
                                   | event_endpoint_updated
                                   | event_delivery_failure.
-type sns_application() :: [{arn, string()} | {attributes, [{arn|sns_application_attribute(), string()}]}].

-type(sns_topic_attribute_name () :: 'Policy' | 'DisplayName' | 'DeliveryPolicy').

-type(sns_subscribe_protocol_type () :: http | https | email | 'email-json' | sms | sqs | application).

-export_type([sns_acl/0, sns_endpoint_attribute/0,
              sns_message/0, sns_application/0, sns_endpoint/0]).

-spec add_permission(string(), string(), sns_acl()) -> ok.

add_permission(TopicArn, Label, Permissions) ->
    add_permission(TopicArn, Label, Permissions, default_config()).

-spec add_permission(string(), string(), sns_acl(), aws_config()) -> ok.
add_permission(TopicArn, Label, Permissions, Config)
  when is_list(TopicArn),
       is_list(Label), length(Label) =< 80,
       is_list(Permissions) ->
    sns_simple_request(Config, "AddPermission",
                       [{"Label", Label}, {"TopicArn", TopicArn}
                       | erlcloud_aws:param_list(
                            encode_permissions(Permissions),
                            {"AWSAccountId.member", "ActionName.member"})]).



-spec create_platform_endpoint(string(), string()) -> string().
create_platform_endpoint(PlatformApplicationArn, Token) ->
    create_platform_endpoint(PlatformApplicationArn, Token, "").

-spec create_platform_endpoint(string(), string(), string()) -> string().
create_platform_endpoint(PlatformApplicationArn, Token, CustomUserData) ->
    create_platform_endpoint(PlatformApplicationArn, Token, CustomUserData, []).

-spec create_platform_endpoint(string(), string(), string(), [{sns_endpoint_attribute(), string()}]) -> string().
create_platform_endpoint(PlatformApplicationArn, Token, CustomUserData, Attributes) ->
    create_platform_endpoint(PlatformApplicationArn, Token, CustomUserData, Attributes, default_config()).

-spec create_platform_endpoint(string(), string(), string(), [{sns_endpoint_attribute(), string()}], aws_config()) -> string().
create_platform_endpoint(PlatformApplicationArn, Token, CustomUserData, Attributes, Config) ->
    Doc =
        sns_xml_request(
            Config, "CreatePlatformEndpoint",
            [{"PlatformApplicationArn", PlatformApplicationArn},
             {"Token",                  Token},
             {"CustomUserData",         CustomUserData}
             | encode_attributes(Attributes)
             ]),
    erlcloud_xml:get_text(
        "CreatePlatformEndpointResult/EndpointArn", Doc).

-spec create_platform_endpoint(string(), string(), string(), [{sns_endpoint_attribute(), string()}], string(), string()) -> string().
create_platform_endpoint(PlatformApplicationArn, Token, CustomUserData, Attributes, AccessKeyID, SecretAccessKey) ->
    create_platform_endpoint(PlatformApplicationArn, Token, CustomUserData, Attributes, new_config(AccessKeyID, SecretAccessKey)).


-spec create_topic(string()) -> string().
create_topic(TopicName) ->
    create_topic(TopicName, default_config()).

-spec create_topic(string(), aws_config()) -> string().
create_topic(TopicName, Config)
    when is_record(Config, aws_config) ->
        Doc = sns_xml_request(Config, "CreateTopic", [{"Name", TopicName}]),
        erlcloud_xml:get_text("/CreateTopicResponse/CreateTopicResult/TopicArn", Doc).


-spec confirm_subscription(sns_event()) -> string().
confirm_subscription(SnsEvent) ->
    confirm_subscription(SnsEvent, default_config()).

-spec confirm_subscription(sns_event(), aws_config()) -> string().
confirm_subscription(SnsEvent, Config) ->
    Token = binary_to_list(proplists:get_value(<<"Token">>, SnsEvent, <<>>)),
    TopicArn = binary_to_list(proplists:get_value(<<"TopicArn">>, SnsEvent, <<>>)),
    confirm_subscription2(Token, TopicArn, Config).

-spec confirm_subscription(sns_event(), string(), string()) -> string().
confirm_subscription(SnsEvent, AccessKeyID, SecretAccessKey) ->
    confirm_subscription(SnsEvent, new_config(AccessKeyID, SecretAccessKey)).



-spec confirm_subscription2(string(), string()) -> string().
confirm_subscription2(Token, TopicArn) ->
    confirm_subscription2(Token, TopicArn, default_config()).

-spec confirm_subscription2(string(), string(), aws_config()) -> string().
confirm_subscription2(Token, TopicArn, Config) ->
    Doc =
        sns_xml_request(
            Config, "ConfirmSubscription",
            [{"Token",    Token},
             {"TopicArn", TopicArn}
             ]),
    erlcloud_xml:get_text(
        "ConfirmSubscriptionResult/SubscriptionArn", Doc).

-spec confirm_subscription2(string(), string(), string(), string()) -> string().
confirm_subscription2(Token, TopicArn, AccessKeyID, SecretAccessKey) ->
    confirm_subscription2(Token, TopicArn, new_config(AccessKeyID, SecretAccessKey)).



-spec delete_endpoint(string()) -> ok.
delete_endpoint(EndpointArn) ->
    delete_endpoint(EndpointArn, default_config()).

-spec delete_endpoint(string(), aws_config()) -> ok.
delete_endpoint(EndpointArn, Config) ->
    sns_simple_request(Config, "DeleteEndpoint", [{"EndpointArn", EndpointArn}]).

-spec delete_endpoint(string(), string(), string()) -> ok.
delete_endpoint(EndpointArn, AccessKeyID, SecretAccessKey) ->
    delete_endpoint(EndpointArn, new_config(AccessKeyID, SecretAccessKey)).



-spec delete_topic(string()) -> ok.
delete_topic(TopicArn) ->
    delete_topic(TopicArn, default_config()).

-spec delete_topic(string(), aws_config()) -> ok.
delete_topic(TopicArn, Config)
    when is_record(Config, aws_config) ->
        sns_simple_request(Config, "DeleteTopic", [{"TopicArn", TopicArn}]).



-spec get_endpoint_attributes(string()) -> sns_endpoint().
get_endpoint_attributes(EndpointArn) ->
    get_endpoint_attributes(EndpointArn, default_config()).

-spec get_endpoint_attributes(string(), aws_config()) -> sns_endpoint().
get_endpoint_attributes(EndpointArn, Config) ->
    Params = [{"EndpointArn", EndpointArn}],
    Doc = sns_xml_request(Config, "GetEndpointAttributes", Params),
    Decoded =
        erlcloud_xml:decode(
            [{attributes, "GetEndpointAttributesResult/Attributes/entry",
                fun extract_attribute/1
             }],
            Doc),
    [{arn, EndpointArn} | Decoded].

-spec get_endpoint_attributes(string(), string(), string()) -> sns_endpoint().
get_endpoint_attributes(EndpointArn, AccessKeyID, SecretAccessKey) ->
    get_endpoint_attributes(EndpointArn, new_config(AccessKeyID, SecretAccessKey)).



-spec set_endpoint_attributes(string(), [{sns_endpoint_attribute(), string()}]) -> string().
set_endpoint_attributes(EndpointArn, Attributes) ->
    set_endpoint_attributes(EndpointArn, Attributes, default_config()).
-spec set_endpoint_attributes(string(), [{sns_endpoint_attribute(), string()}], aws_config()) -> string().
set_endpoint_attributes(EndpointArn, Attributes, Config) ->
    Doc = sns_xml_request(Config, "SetEndpointAttributes", [{"EndpointArn", EndpointArn} |
                                                            encode_attributes(Attributes)]),
    erlcloud_xml:get_text("ResponseMetadata/RequestId", Doc).



-spec list_endpoints_by_platform_application(string()) -> [{endpoints, [sns_endpoint()]} | {next_token, string()}].
list_endpoints_by_platform_application(PlatformApplicationArn) ->
    list_endpoints_by_platform_application(PlatformApplicationArn, undefined).
-spec list_endpoints_by_platform_application(string(), undefined|string()) -> [{endpoints, [sns_endpoint()]} | {next_token, string()}].
list_endpoints_by_platform_application(PlatformApplicationArn, NextToken) ->
    list_endpoints_by_platform_application(PlatformApplicationArn, NextToken, default_config()).
-spec list_endpoints_by_platform_application(string(), undefined|string(), aws_config()) -> [{endpoints, [sns_endpoint()]} | {next_token, string()}].
list_endpoints_by_platform_application(PlatformApplicationArn, NextToken, Config) ->
    Params =
        case NextToken of
            undefined -> [{"PlatformApplicationArn", PlatformApplicationArn}];
            NextToken -> [{"PlatformApplicationArn", PlatformApplicationArn}, {"NextToken", NextToken}]
        end,
    Doc = sns_xml_request(Config, "ListEndpointsByPlatformApplication", Params),
    Decoded =
        erlcloud_xml:decode(
            [{endpoints, "ListEndpointsByPlatformApplicationResult/Endpoints/member",
                fun extract_endpoint/1
             },
             {next_token, "ListEndpointsByPlatformApplicationResult/NextToken", text}],
            Doc),
    Decoded.
-spec list_endpoints_by_platform_application(string(), undefined|string(), string(), string()) -> [{endpoints, [sns_endpoint()]} | {next_token, string()}].
list_endpoints_by_platform_application(PlatformApplicationArn, NextToken, AccessKeyID, SecretAccessKey) ->
    list_endpoints_by_platform_application(PlatformApplicationArn, NextToken, new_config(AccessKeyID, SecretAccessKey)).

-spec list_topics() -> [{topics, [[{arn, string()}]]} | {next_token, string()}].
list_topics() ->
    list_topics(default_config()).

-spec list_topics(undefined | string() | aws_config()) -> [{topics, [[{arn, string()}]]} | {next_token, string()}].
list_topics(Config) when is_record(Config, aws_config) ->
    list_topics(undefined, Config);
list_topics(NextToken) ->
    list_topics(NextToken, default_config()).

-spec list_topics(undefined | string(), aws_config()) ->  [{topics, [[{arn, string()}]]} | {next_token, string()}].
list_topics(NextToken, Config) ->
    Params =
        case NextToken of
            undefined -> [];
            NextToken -> [{"NextToken", NextToken}]
        end,
    Doc = sns_xml_request(Config, "ListTopics", Params),
    Decoded =
        erlcloud_xml:decode(
            [{topics, "ListTopicsResult/Topics/member",
                fun extract_topic_arn/1
             },
             {next_token, "ListTopicsResult/NextToken", text}],
            Doc),
    Decoded.

-spec list_topics_all() -> [[{arn, string()}]].
list_topics_all() ->
    list_topics_all(default_config()).

-spec list_topics_all(aws_config()) -> [[{arn, string()}]].
list_topics_all(Config) ->
    list_all(fun list_topics/2, topics, Config, undefined, []).


-spec list_platform_applications() -> [sns_application()].
list_platform_applications() ->
    list_platform_applications(undefined).

-spec list_platform_applications(undefined|string()) -> [sns_application()].
list_platform_applications(NextToken) ->
    list_platform_applications(NextToken, default_config()).

-spec list_platform_applications(undefined|string(), aws_config()) -> [sns_application()].
list_platform_applications(NextToken, Config) ->
    Params =
        case NextToken of
            undefined -> [];
            NextToken -> [{"NextToken", NextToken}]
        end,
    Doc = sns_xml_request(Config, "ListPlatformApplications", Params),
    Decoded =
        erlcloud_xml:decode(
            [{applications, "ListPlatformApplicationsResult/PlatformApplications/member",
                fun extract_application/1
             }],
            Doc),
    proplists:get_value(applications, Decoded, []).

-spec list_platform_applications(undefined|string(), string(), string()) -> [sns_application()].
list_platform_applications(NextToken, AccessKeyID, SecretAccessKey) ->
    list_platform_applications(NextToken, new_config(AccessKeyID, SecretAccessKey)).



-spec publish_to_topic(string(), sns_message()) -> string().
publish_to_topic(TopicArn, Message) ->
    publish_to_topic(TopicArn, Message, undefined).

-spec publish_to_topic(string(), sns_message(), undefined|string()) -> string().
publish_to_topic(TopicArn, Message, Subject) ->
    publish_to_topic(TopicArn, Message, Subject, default_config()).

-spec publish_to_topic(string(), sns_message(), undefined|string(), aws_config()) -> string().
publish_to_topic(TopicArn, Message, Subject, Config) ->
    publish(topic, TopicArn, Message, Subject, Config).

-spec publish_to_topic(string(), sns_message(), undefined|string(), string(), string()) -> string().
publish_to_topic(TopicArn, Message, Subject, AccessKeyID, SecretAccessKey) ->
    publish_to_topic(TopicArn, Message, Subject, new_config(AccessKeyID, SecretAccessKey)).

-spec publish_to_target(string(), sns_message()) -> string().
publish_to_target(TargetArn, Message) ->
    publish_to_target(TargetArn, Message, undefined).

-spec publish_to_target(string(), sns_message(), undefined|string()) -> string().
publish_to_target(TargetArn, Message, Subject) ->
    publish_to_target(TargetArn, Message, Subject, default_config()).

-spec publish_to_target(string(), sns_message(), undefined|string(), aws_config()) -> string().
publish_to_target(TargetArn, Message, Subject, Config) ->
    publish(target, TargetArn, Message, Subject, Config).

-spec publish_to_target(string(), sns_message(), undefined|string(), string(), string()) -> string().
publish_to_target(TargetArn, Message, Subject, AccessKeyID, SecretAccessKey) ->
    publish_to_target(TargetArn, Message, Subject, new_config(AccessKeyID, SecretAccessKey)).

-spec publish(topic|target, string(), sns_message(), undefined|string(), aws_config()) -> string().
publish(Type, RecipientArn, Message, Subject, Config) ->
    RecipientParam =
        case Type of
            topic -> [{"TopicArn", RecipientArn}];
            target -> [{"TargetArn", RecipientArn}]
        end,
    MessageParams =
        case Message of
            [{_,_} |_] ->
                EncodedMessage = jsx:encode(Message),
                [{"Message",            EncodedMessage},
                 {"MessageStructure",   "json"}];
            Message ->
                [{"Message", Message}]
        end,
    SubjectParam =
        case Subject of
            undefined -> [];
            Subject -> [{"Subject", Subject}]
        end,
    Doc =
        sns_xml_request(
            Config, "Publish",
            RecipientParam ++ MessageParams ++ SubjectParam),
    erlcloud_xml:get_text(
        "PublishResult/MessageId", Doc).



-spec parse_event(iodata()) -> sns_event().
parse_event(EventSource) ->
    jsx:decode(EventSource).

-spec get_event_type(sns_event()) -> sns_event_type().
get_event_type(Event) ->
    case proplists:get_value(<<"Type">>, Event) of
        <<"SubscriptionConfirmation">> -> subscription_confirmation;
        <<"Notification">> -> notification
    end.

-spec parse_event_message(sns_event()) -> sns_notification() | binary().
parse_event_message(Event) ->
    Message = proplists:get_value(<<"Message">>, Event, <<>>),
    case get_event_type(Event) of
        subscription_confirmation -> Message;
        notification -> jsx:decode(Message)
    end.

-spec get_notification_attribute(binary(), sns_notification()) -> sns_application_attribute() | binary().
get_notification_attribute(<<"EventType">>, Notification) ->
    case proplists:get_value(<<"EventType">>, Notification) of
        <<"EndpointCreated">> -> event_endpoint_created;
        <<"EndpointDeleted">> -> event_endpoint_deleted;
        <<"EndpointUpdated">> -> event_endpoint_updated;
        <<"DeliveryFailure">> -> event_delivery_failure
    end;
get_notification_attribute(Attribute, Notification) ->
    proplists:get_value(Attribute, Notification).



-spec set_topic_attributes(sns_topic_attribute_name(), string(), string()) -> ok.
set_topic_attributes(AttributeName, AttributeValue, TopicArn) ->
    set_topic_attributes(AttributeName, AttributeValue, TopicArn, default_config()).

-spec set_topic_attributes(sns_topic_attribute_name(), string(), string(), aws_config()) -> ok.
set_topic_attributes(AttributeName, AttributeValue, TopicArn, Config)
    when is_record(Config, aws_config) ->
        sns_simple_request(Config, "SetTopicAttributes", [
            {"AttributeName", AttributeName},
            {"AttributeValue", AttributeValue},
            {"TopicArn", TopicArn}]).


-spec subscribe(string(), sns_subscribe_protocol_type(), string()) -> Arn::string().
subscribe(Endpoint, Protocol, TopicArn) ->
    subscribe(Endpoint, Protocol, TopicArn, default_config()).

-spec subscribe(string(), sns_subscribe_protocol_type(), string(), aws_config()) -> Arn::string().
subscribe(Endpoint, Protocol, TopicArn, Config)
    when is_record(Config, aws_config) ->
         Doc = sns_xml_request(Config, "Subscribe", [
                {"Endpoint", Endpoint},
                {"Protocol", atom_to_list(Protocol)},
                {"TopicArn", TopicArn}]),
        erlcloud_xml:get_text("/SubscribeResponse/SubscribeResult/SubscriptionArn", Doc).


-spec unsubscribe( string()) -> string().
unsubscribe(SubscriptionArn) ->
        unsubscribe(SubscriptionArn, default_config()).

-spec unsubscribe(string(), aws_config()) -> string().
unsubscribe(SubscriptionArn, Config) when is_record(Config, aws_config) ->
    Doc = sns_xml_request(Config, "Unsubscribe", [
                                                  {"SubscriptionArn", SubscriptionArn}]),
    erlcloud_xml:get_text("/UnsubscribeResponse/ResponseMetadata/RequestId", Doc).


-spec new(string(), string()) -> aws_config().

new(AccessKeyID, SecretAccessKey) ->
    #aws_config{
       access_key_id=AccessKeyID,
       secret_access_key=SecretAccessKey
      }.

-spec new(string(), string(), string()) -> aws_config().

new(AccessKeyID, SecretAccessKey, Host) ->
    #aws_config{
       access_key_id=AccessKeyID,
       secret_access_key=SecretAccessKey,
       sns_host=Host
      }.

-spec configure(string(), string()) -> ok.

configure(AccessKeyID, SecretAccessKey) ->
    put(aws_config, new(AccessKeyID, SecretAccessKey)),
    ok.

-spec configure(string(), string(), string()) -> ok.

configure(AccessKeyID, SecretAccessKey, Host) ->
    put(aws_config, new(AccessKeyID, SecretAccessKey, Host)),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% PRIVATE
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

encode_attributes(Attributes) ->
    lists:foldl(
        fun({Index, {Key, Value}}, Acc) ->
            Prefix = "Attributes.entry." ++ integer_to_list(Index),
            [{Prefix ++ ".key",     encode_attribute_name(Key)},
             {Prefix ++ ".value",   Value} | Acc]
        end, [], lists:zip(lists:seq(1, length(Attributes)), Attributes)).

encode_attribute_name(custom_user_data) -> "CustomUserData";
encode_attribute_name(enabled) -> "Enabled";
encode_attribute_name(token) -> "Token".

encode_permissions(Permissions) ->
    [encode_permission(P) || P <- Permissions].

encode_permission({AccountId, Permission}) ->
    {AccountId,
     case Permission of
         all -> "*";
         add_permission -> "AddPermission";
         confirm_subscription -> "ConfirmSubscription";
         create_platform_application -> "CreatePlatformApplication";
         create_platform_endpoint -> "CreatePlatformEndpoint";
         create_topic -> "CreateTopic";
         delete_endpoint -> "DeleteEndpoint";
         delete_platform_application -> "DeletePlatformApplication";
         delete_topic -> "DeleteTopic";
         get_endpoint_attributes -> "GetEndpointAttributes";
         get_platform_application_attributes -> "GetPlatformApplicationAttributes";
         get_subscription_attributes -> "GetSubscriptionAttributes";
         get_topic_attributes -> "GetTopicAttributes";
         list_endpoints_by_platform_application -> "ListEndpointsByPlatformApplication";
         list_platform_applications -> "ListPlatformApplications";
         list_subscriptions -> "ListSubscriptions";
         list_subscriptions_by_topic -> "ListSubscriptionsByTopic";
         list_topics -> "ListTopics";
         publish -> "Publish";
         remove_permission -> "RemovePermission";
         set_endpoint_attributes -> "SetEndpointAttributes";
         set_platform_application_attributes -> "SetPlatformApplicationAttributes";
         set_subscription_attributes -> "SetSubscriptionAttributes";
         set_topic_attributes -> "SetTopicAttributes";
         subscribe -> "Subscribe";
         unsubscribe -> "Unsubscribe"
      end}.



default_config() -> erlcloud_aws:default_config().

new_config(AccessKeyID, SecretAccessKey) ->
    #aws_config{
       access_key_id=AccessKeyID,
       secret_access_key=SecretAccessKey
      }.

sns_simple_request(Config, Action, Params) ->
    sns_request(Config, Action, Params),
    ok.

sns_xml_request(Config, Action, Params) ->
    case erlcloud_aws:aws_request_xml4(post,
                                       scheme_to_protocol(Config#aws_config.sns_scheme),
                                       Config#aws_config.sns_host, undefined, "/",
                                       [{"Action", Action}, {"Version", ?API_VERSION} | Params],
                                       "sns", Config) of
        {ok, XML} -> XML;
        {error, {http_error, 400, _BadRequest, Body}} ->
            XML = element(1, xmerl_scan:string(binary_to_list(Body))),
            ErrCode = erlcloud_xml:get_text("Error/Code", XML),
            ErrMsg = erlcloud_xml:get_text("Error/Message", XML),
            erlang:error({sns_error, ErrCode, ErrMsg});
        {error, Reason} ->
            erlang:error({sns_error, Reason})
    end.

sns_request(Config, Action, Params) ->
    case erlcloud_aws:aws_request_xml4(post,
                                       scheme_to_protocol(Config#aws_config.sns_scheme),
                                       Config#aws_config.sns_host, undefined, "/",
                                       [{"Action", Action}, {"Version", ?API_VERSION} | Params],
                                       "sns", Config) of
        {ok, _Response} -> ok;
        {error, {http_error, 400, _BadRequest, Body}} ->
            XML = element(1, xmerl_scan:string(binary_to_list(Body))),
            ErrCode = erlcloud_xml:get_text("Error/Code", XML),
            ErrMsg = erlcloud_xml:get_text("Error/Message", XML),
            erlang:error({sns_error, ErrCode, ErrMsg});
        {error, Reason} ->
            erlang:error({sns_error, Reason})
    end.

list_all(Fun, Type, Config, Token, Acc) ->
    Res = Fun(Token, Config),
    List = proplists:get_value(Type, Res),
    case proplists:get_value(next_token, Res) of
        "" ->
            lists:foldl(fun erlang:'++'/2, [], [List | Acc]);
        NextToken ->
            list_all(Fun, Type, Config, NextToken, [List | Acc])
    end.

extract_endpoint(Nodes) ->
    [erlcloud_xml:decode(
        [{arn, "EndpointArn", text},
         {attributes, "Attributes/entry", fun extract_attribute/1}
        ], Node) || Node <- Nodes].

extract_application(Nodes) ->
    [erlcloud_xml:decode(
        [{arn, "PlatformApplicationArn", text},
         {attributes, "Attributes/entry", fun extract_attribute/1}
        ], Node) || Node <- Nodes].

extract_attribute(Nodes) ->
    [{parse_key(erlcloud_xml:get_text("key", Node)),
      erlcloud_xml:get_text("value", Node)}
     || Node <- Nodes].

extract_topic_arn(Nodes) ->
    [erlcloud_xml:decode([{arn, "TopicArn", text}], Node) || Node <- Nodes].

parse_key("Enabled") -> enabled;
parse_key("CustomUserData") -> custom_user_data;
parse_key("Token") -> token;
parse_key("EventEndpointCreated") -> event_endpoint_created;
parse_key("EventEndpointDeleted") -> event_endpoint_deleted;
parse_key("EventEndpointUpdated") -> event_endpoint_updated;
parse_key("EVentDeliveryFailure") -> event_delivery_failure;
parse_key(OtherKey) -> list_to_atom(string:to_lower(OtherKey)).

scheme_to_protocol(S) when is_list(S) -> s2p(string:to_lower(S));
scheme_to_protocol(_)                 -> erlang:error({sns_error, badarg}).

s2p("http://")  -> "http";
s2p("https://") -> "https";
s2p(X)          -> erlang:error({sns_error, {unsupported_scheme, X}}).
