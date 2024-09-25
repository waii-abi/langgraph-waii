import uuid
from io import StringIO

import pandas as pd
from langgraph.constants import START, END
from langgraph.graph import StateGraph
from langgraph.types import Send
from waii_sdk_py import WAII
from waii_sdk_py.query import QueryGenerationRequest, RunQueryRequest


class State(dict):
    def __init__(self, **kwargs):
        super().__init__()
        self['query'] = kwargs.get('query', '')
        self['sql'] = kwargs.get('sql', '')
        self['data'] = kwargs.get('data', [])
        self['chart'] = kwargs.get('chart', '')
        self['insight'] = kwargs.get('insight', '')
        self['response'] = kwargs.get('response', '')
        self['error'] = kwargs.get('error', '')
        self['attempts'] = kwargs.get('attempts', 0)
        # for conditional branching
        self['path_decision'] = ""


def intent_classifier(state: State) -> State:
    print(f"Classifying and processing query: {state['query']}")

    waii_intent_classification(query=state["query"])

    state["query"] = f"Processed: {state['query']}"
    state["error"] = None
    return state


def sql_generator(state: State) -> State:
    print(f"Generating SQL for query: {state['query']}")
    try:
        state["sql"] = waii_sql_generator(question=state["query"])
        state["error"] = None
    except Exception as e:
        state["error"] = str(e)
    return state


def sql_executor(state: State) -> State:
    print(f"Executing query: {state['sql']}")
    if state.get("error"):
        return state
    try:
        state["data"] = waii_sql_executor(query=state["sql"])
        state["error"] = None
    except Exception as e:
        state["error"] = str(e)
    return state


def chart_generator(state: State) -> State:
    print(f"Generating chart for data: {state['data']}")
    if state.get("error"):
        return state
    try:
        state["chart"] = waii_chart_generator(state["data"])
        state["error"] = None
    except Exception as e:
        state["error"] = str(e)
    return state


def insight_generator(state: State) -> State:
    print(f"Generating insight for data: {state['data']}")
    if state.get("error"):
        return state
    # TODO: Need to fix this for integration with WAII
    state["insight"] = waii_insight_generator(state["data"])
    return state


def result_synthesizer(state: State) -> State:
    print(f"Formulating response with insight")
    if state.get("error"):
        return state
    # TODO: Need to fix this for integration with WAII
    state["response"] = f"Here is the formulated response"
    state["error"] = None
    return state


def decision_step(state: State) -> State:
    print("Deciding the next step based on the query result...")

    # Example decision logic: If 'data' has more than one row, generate a chart.
    if len(state['data']) > 1:
        print("Decision: Generating chart")
        state['path_decision'] = "chart_generator"
    else:
        print("Decision: Generating insight")
        state['path_decision'] = "insight_generator"
    return state


def decision_step_conditional_branch(state: State):
    # Log the decision to ensure the path decision is correct
    print(f"Routing based on path_decision: {state['path_decision']}")

    # Use send() to send the state to next node
    if state['path_decision'] == "chart_generator":
        return [Send("chart_generator", state)]
    elif state['path_decision'] == "insight_generator":
        return [Send("insight_generator", state)]
    else:
        raise ValueError(f"Unknown path_decision: {state['path_decision']}")


def should_retry_generic(state: State, success_next: str, retry_next: str = "sql_generator"):
    print(f"\tChecking if we should retry in {success_next}: error: {state.get('error')}")
    if state.get("error", None) and state.get("attempts", 0) < 3:
        state["attempts"] = state.get("attempts", 0) + 1
        state["error"] = None
        return [Send(retry_next, state)]

    # Reset state on success
    state["error"] = None
    state["attempts"] = 0
    print(f"Moving on to {success_next}")
    return [Send(retry_next, state)]


def create_retry_func(success_next: str):
    return lambda state: should_retry_generic(state, success_next)


def init_waii():
    url = 'http://localhost:9859/api/'
    api_key = ''
    # db_connection_str = 'snowflake://SF_USER@SF_ACCOUNT_NAME/MOVIE_DB?role=SF_ROLE&warehouse=COMPUTE_WH'
    # TODO: Disable the following line later.
    db_connection_str = 'snowflake://WAII_USER@gqobxjv-bhb91428/MOVIE_DB?role=WAII_USER_ROLE&warehouse=COMPUTE_WH'
    WAII.initialize(url=url, api_key=api_key)
    WAII.Database.activate_connection(db_connection_str)
    print(f"Initialized WAII with connection: {db_connection_str}")


def waii_intent_classification(query: str) -> str:
    # TODO: Placeholder for intent classification
    return "Something"


def waii_sql_generator(question: str) -> str:
    try:
        query_id = str(uuid.uuid4())
        response = WAII.Query.generate(QueryGenerationRequest(uuid=query_id, ask=question))
        return response.query
    except Exception as e:
        print(f"Error generating query: {e}")
        return ""


def waii_sql_executor(query: str) -> str:
    try:
        response = WAII.Query.run(RunQueryRequest(query=query))
        print(f"Executed the query, num of rows: {len(response.rows)}")
        return response.rows
    except Exception as e:
        print(f"Error executing query: {e}")
        return ""


def waii_chart_generator(data: str) -> str:
    try:
        # TODO: Need to convert data to df. This is dummy impl.
        df_data = pd.read_csv(StringIO(data))
        response = WAII.Chart.chart_generator(df=df_data)
        print(f"Chart spec: {response.chart_spec}")
        return response.chart_spec
    except Exception as e:
        print(f"Error generating chart: {e}")
        raise e

def waii_insight_generator(param: str) -> str:
    # TODO: Need to integrate with WAII for generating query
    return "Insight: These are the top 5 directors."


# Switch to StateGraph to handle multiple edges
workflow = StateGraph(State)

# Add nodes to the graph
workflow.add_node("Intent Classifier", intent_classifier)
workflow.add_node("SQL Generator", sql_generator)
workflow.add_node("SQL Executor", sql_executor)
workflow.add_node("Chart Generator", chart_generator)
workflow.add_node("Insight Generator", insight_generator)
workflow.add_node("Result Synthesizer", result_synthesizer)
#workflow.add_node("Error Handler", decision_step)

# Define edges to control workflow execution
workflow.add_edge(START, "Intent Classifier")
workflow.add_edge("Intent Classifier", "SQL Generator")
workflow.add_edge("Intent Classifier", "Chart Generator")
workflow.add_edge("Intent Classifier", "Insight Generator")
workflow.add_edge("SQL Generator", "SQL Executor")
workflow.add_edge("SQL Executor", "Result Synthesizer")
workflow.add_edge("Chart Generator", "Result Synthesizer")
workflow.add_edge("Insight Generator", "Result Synthesizer")
workflow.add_edge("Result Synthesizer", END)

# Add conditional edge for error handling in respective nodes
# TODO: Disabling it as there were issues in sending state correctly.
#workflow.add_conditional_edges("sql_generator", create_retry_func("sql_executor"))
#workflow.add_conditional_edges("sql_executor", create_retry_func("decision_step"))
#workflow.add_conditional_edges("chart_generator", create_retry_func("result_synthesizer"))
#workflow.add_conditional_edges("insight_generator", create_retry_func("result_synthesizer"))

# Add conditional edge from decision step so that we can jump to either chart_generator or insight_generator
#workflow.add_conditional_edges("decision_step", decision_step_conditional_branch)

# init WAII related APIs.
init_waii()

# Compile the workflow
app = workflow.compile(debug=False)

# Print the workflow graph.
print(app.get_graph().draw_ascii())


def run_workflow(query: str) -> str:
    initial_state = State(query=query, attempts=0)
    final_state = app.invoke(initial_state)
    return final_state["response"]


# Example usage
response = run_workflow("Who are the top 5 directors with the highest number of titles?")
print(response)
