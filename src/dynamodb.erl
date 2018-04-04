-module(dynamodb).

-define(FULLDYNAMODB_AWS_IAM_ACCESSKEY, "AKIAJX3L2K7Q5FTMCGJQ").
-define(FULLDYNAMODB_AWS_IAM_ACCESSKEY_SECRET,"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").
-define(NULL_STRING, <<"empty_string">>).
-define(NULL_LIST, [<<"empty_list">>]).
-define(DYNAMODB_HOST,os:getenv("DYNAMODB_HOST","localhost")).
-define(DYNAMODB_PORT,list_to_integer(os:getenv("DYNAMODB_PORT","8080"))).
-define(DYNAMODB_SCHEME,os:getenv("DYNAMODB_SCHEME","http://")).

-include_lib("erlcloud/include/erlcloud.hrl").
-include_lib("erlcloud/include/erlcloud_aws.hrl").
-include_lib("erlcloud/include/erlcloud_ddb2.hrl").

-export([start/0, 
	 generate_dynamodb_put_item/2,
	 generate_dynamodb_delete_item/2, 
	 batch_write_item/2,
	 update_item/4, 
	 update_adding_values/3, 
	 update_setting_values/3,
         update_adding_values_v0/3,
         update_setting_values_v0/3, 
	 get_item/3]).

-spec start() -> ok | error.
start() ->
    %%ssl:start(),
    erlcloud:start(),
    erlcloud_ddb2:configure(?FULLDYNAMODB_AWS_IAM_ACCESSKEY,?FULLDYNAMODB_AWS_IAM_ACCESSKEY_SECRET,?DYNAMODB_HOST,?DYNAMODB_PORT,?DYNAMODB_SCHEME).


batch_write_item(Batch, Opts) ->
    erlcloud_ddb2:configure(?FULLDYNAMODB_AWS_IAM_ACCESSKEY,?FULLDYNAMODB_AWS_IAM_ACCESSKEY_SECRET,?DYNAMODB_HOST,?DYNAMODB_PORT,?DYNAMODB_SCHEME),
    case erlcloud_ddb2:batch_write_item(Batch, Opts) of 
	{ok,_} -> ok;
	{_, R} -> io:format("Error Batch Write ~p~n",[R])
    end.



update_adding_values_v0(Table, Key, TupleList) ->
    { UpdateExpression, ValueListMain } = make_update_expression_for(<<"ADD ">>, TupleList, 1,[],[]),
    ValueList = [{ <<":v0">>, 0 } | ValueListMain],
    Opts = [{expression_attribute_values, ValueList}, {return_values, all_new}],
    update_item(Table, Key, UpdateExpression, Opts).

update_setting_values_v0(Table, Key, TupleList) ->
    { UpdateExpression, ValueListMain } = make_update_expression_for(<<"SET ">>, TupleList, 1,[],[]),
    ValueList = [{ <<":v0">>, 0 }| ValueListMain],
    %%io:format("Update Expression: ~p~n Value List: ~p~n",[UpdateExpression, ValueList]),                                                                                                                                                                                    
    Opts = [{expression_attribute_values, ValueList}, {return_values, all_new}],
    update_item(Table, Key, UpdateExpression, Opts).

update_adding_values(Table, Key, TupleList) ->
    { UpdateExpression, ValueList } = make_update_expression_for(<<"ADD ">>, TupleList, 1,[],[]),
    Opts = [{expression_attribute_values, ValueList}, {return_values, all_new}],
    update_item(Table, Key, UpdateExpression, Opts).

update_setting_values(Table, Key, TupleList) ->
    { UpdateExpression, ValueList } = make_update_expression_for(<<"SET ">>, TupleList, 1,[],[]),
    %%io:format("Update Expression: ~p~n Value List: ~p~n",[UpdateExpression, ValueList]),
    Opts = [{expression_attribute_values, ValueList}, {return_values, all_new}],
    update_item(Table, Key, UpdateExpression, Opts).

make_update_expression_for(Op, [{K,V}| [] ], Count, ExpressionList, ValueList) ->
    NewExpressionList = [iolist_to_binary([K, <<" :v">>,integer_to_binary(Count)]) | ExpressionList],
    NewValueList = [ { iolist_to_binary([<<":v">>, integer_to_binary(Count) ] ), check_empty(V) }  | ValueList ],
    UpdateExpression = iolist_to_binary([Op | lists:reverse(NewExpressionList)]),
    {UpdateExpression, NewValueList};

make_update_expression_for(Op, [{K,V}|T], Count, ExpressionList, ValueList) ->
    NewExpressionList = [iolist_to_binary([K,<<" :v">>,integer_to_binary(Count), <<", ">>]) | ExpressionList],
    NewValueList = [ { iolist_to_binary([<<":v">>, integer_to_binary(Count) ] ), check_empty(V) }  | ValueList ],
    make_update_expression_for(Op, T,Count+1, NewExpressionList, NewValueList).


check_empty([]) -> ?NULL_LIST;
check_empty(<<>>) -> ?NULL_STRING;
check_empty(V) -> V.

update_item(Table, Key, UpdatesOrExpression, Opts) ->
    erlcloud_ddb2:configure(?FULLDYNAMODB_AWS_IAM_ACCESSKEY,?FULLDYNAMODB_AWS_IAM_ACCESSKEY_SECRET,?DYNAMODB_HOST,?DYNAMODB_PORT,?DYNAMODB_SCHEME),
    case erlcloud_ddb2:update_item(Table, Key, UpdatesOrExpression, Opts) of
        {ok,_} ->
	    ok;
	{_, R} -> io:format("Error Update Item ~p~n",[R])
    end.
 
get_item(Table, Key,  Opts) ->
    erlcloud_ddb2:configure(?FULLDYNAMODB_AWS_IAM_ACCESSKEY,?FULLDYNAMODB_AWS_IAM_ACCESSKEY_SECRET,?DYNAMODB_HOST,?DYNAMODB_PORT,?DYNAMODB_SCHEME),
    erlcloud_ddb2:get_item(Table, Key,  Opts).


generate_dynamodb_put_item(PropList, PropValueList) ->
    {put, lists:zip([define_attr(A) || A <- PropList ] , [define_value(V) || V <- PropValueList ] )}. 

generate_dynamodb_delete_item(PropList, PropValueList) ->
    {delete, lists:zip([define_attr(A) || A <- PropList ] , [define_value(V) || V <- PropValueList ] )}.


define_value(V) when is_integer(V) ->
    {n, V};
define_value(V) when is_float(V) -> {n, V};
define_value(V) when is_binary(V) -> {s, V};
define_value(V) when is_list(V) andalso is_tuple(hd(V)) -> {m, map_from_tuple_list(V,[])};
define_value(V) when is_list(V) ->  {l, [define_value(A) || A <- V ]};
define_value(V) when is_boolean(V) ->{bool,V}.

map_from_tuple_list([], Acc) -> Acc; 
map_from_tuple_list([ {A, V} | T ], Acc) ->
    map_from_tuple_list(T, [{A, define_value(V)} | Acc]).




define_attr(A) when is_atom(A) ->
    atom_to_binary(A, utf8);
define_attr(A) when is_list(A) ->
    list_to_binary(A);
define_attr(A) -> A.
