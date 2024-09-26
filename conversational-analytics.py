import sys
import uuid
from typing import List, Optional, Dict, Any

import pandas as pd
from langgraph.graph import StateGraph
from langgraph.types import Send
from pydantic import BaseModel
from waii_sdk_py import WAII
from waii_sdk_py.query import QueryGenerationRequest, RunQueryRequest

import open_ai_utils


class State(BaseModel):
    query: str = ''
    sql: str = ''
    data: List[Dict[str, Any]] = []
    chart: str = ''
    insight: str = ''
    response: str = ''
    error: Optional[str] = None
    attempts: int = 0
    path_decision: str = ""


class LanggraphWorkflowManager:

    def __init__(self):
        self.workflow = StateGraph(State)
        self.workflow = self.create_workflow()
        self.app = self.workflow.compile()
        self.init_waii()
        print(f"Initialized Langgraph workflow:")
        print(self.app.get_graph().draw_ascii())

    def init_waii(self):
        url = 'http://localhost:9859/api/'
        api_key = ''
        db_connection_str = 'snowflake://WAII_USER@gqobxjv-bhb91428/MOVIE_DB?role=WAII_USER_ROLE&warehouse=COMPUTE_WH'
        WAII.initialize(url=url, api_key=api_key)
        WAII.Database.activate_connection(db_connection_str)
        print(f"Initialized WAII with connection: {db_connection_str}")

    def create_workflow(self) -> StateGraph:
        workflow = StateGraph(State)

        # Add nodes to the graph
        workflow.add_node("Intent Classifier", self.intent_classifier)
        workflow.add_node("SQL Generator", self.sql_generator)
        workflow.add_node("SQL Executor", self.sql_executor)
        workflow.add_node("Chart Generator", self.chart_gen)
        workflow.add_node("Insight Generator", self.insight_generator)
        workflow.add_node("Result Synthesizer", self.result_synthesizer)
        workflow.add_node("Unknown Handler", self.unknown_handler)

        # Define edges to control workflow execution
        workflow.set_entry_point("Intent Classifier")
        workflow.add_conditional_edges(
            "Intent Classifier",
            lambda state: state.path_decision,
            {
                "sql": "SQL Generator",
                "data_visualization": "Chart Generator",
                "insight": "Insight Generator",
                "unknown": "Unknown Handler"
            }
        )
        workflow.add_edge("SQL Generator", "SQL Executor")
        workflow.add_edge("SQL Executor", "Result Synthesizer")
        workflow.add_edge("Chart Generator", "Result Synthesizer")
        workflow.add_edge("Insight Generator", "Result Synthesizer")

        # Loop through the workflow
        workflow.add_edge("Result Synthesizer", "Intent Classifier")
        workflow.add_edge("Unknown Handler", "Intent Classifier")

        return workflow

    def get_user_input(self):
        query = input("Enter your question (Type 'exit' to quit): ")
        if query.lower() == "exit":
            print(f"Exiting...")
            sys.exit()
        return query

    def intent_classifier(self, state: State) -> State:

        state.query = self.get_user_input()

        # Classify the question to one of sql, insight, data_visualization, or unknown
        intent = self.waii_intent_classification(query=state.query)

        if intent in ["sql", "insight", "data_visualization", "unknown"]:
            return state.model_copy(update={"path_decision": intent, "error": None})

    def unknown_handler(self, state: State) -> State:
        print(f"Unable to classify your question. Please enter a valid question.")
        # reset the state and get back to the intent classifier again
        return State()

    def sql_generator(self, state: State) -> State:
        print(f"Generating SQL for query: {state.query}")
        if state.error:
            return state
        try:
            sql = self.waii_sql_generator(question=state.query)
            return state.model_copy(update={"sql": sql})
        except Exception as e:
            return state.model_copy(update={"error": str(e)})

    def sql_executor(self, state: State) -> State:
        print(f"Executing query: {state.sql}")
        if state.error:
            return state
        try:
            data = self.waii_sql_executor(query=state.sql)
            updated_state = state.model_copy(update={"data": data}, deep=True)
            print(f"State after exec: {updated_state}")
            return updated_state
        except Exception as e:
            return state.model_copy(update={"error": str(e)})

    def chart_gen(self, state: State) -> State:
        print(f"Generating chart for data: {state.data}")
        if state.error:
            return state
        try:
            chart = self.waii_chart_generator(state.data)
            return state.model_copy(update={"chart": str(chart), "error": None}, deep=True)
        except Exception as e:
            return state.model_copy(update={"error": str(e)})

    def insight_generator(self, state: State) -> dict:
        print(f"Generating insight for data: {state.data}")
        if state.error:
            return {}
        # TODO: Need to fix this for integration with WAII
        insight = self.waii_insight_generator(state.data)
        return {"insight": insight}

    def result_synthesizer(self, state: State) -> State:
        print(f"Formulating response with insight")
        if state.error:
            print(f"Error in previous step: {state.error}")
            return state
        # Create a response based on the data
        response = "Here are the results of your query:\n"
        for row in state.data:
            response += " | ".join([f"{key}: {value}" for key, value in row.items()])
            response += "\n"
        print(f"Response: {response}")
        return state.model_copy(update={"response": response}, deep=True)

    def decision_step(self, state: State) -> dict:
        print("Deciding the next step based on the query result...")
        # Example decision logic: If 'data' has more than one row, generate a chart.
        if len(state.data) > 1:
            print("Decision: Generating chart")
            return {"path_decision": "chart_generator"}
        else:
            print("Decision: Generating insight")
            return {"path_decision": "insight_generator"}

    def decision_step_conditional_branch(self, state: State):
        print(f"Routing based on path_decision: {state.path_decision}")
        if state.path_decision == "chart_generator":
            return [Send("Chart Generator", state)]
        elif state.path_decision == "Insight Generator":
            return [Send("Insight Generator", state)]
        else:
            raise ValueError(f"Unknown path_decision: {state.path_decision}")

    def should_retry_generic(self, state: State, success_next: str, retry_next: str = "SQL Generator"):
        print(f"Checking if we should retry in {success_next}: error: {state.error}")
        if state.error and state.attempts < 3:
            return [Send(retry_next, State(**state.dict(), attempts=state.attempts + 1, error=None))]
        print(f"Moving on to {success_next}")
        return [Send(success_next, State(**state.dict(), error=None, attempts=0))]

    def create_retry_func(self, success_next: str):
        return lambda state: self.should_retry_generic(state, success_next)

    def waii_intent_classification(self, query: str) -> str:
        system_message = """You are an expert in classifying questions into 'sql', 'data_visualization', 'insight', or 'others'."""
        question = f"Can you classify the following question into one of these categories? Question: '{query}'. " \
                   f"Output: Strictly respond with either 'sql', 'data_visualization', 'insight', or 'unknown'. No additional text."

        classification = open_ai_utils.run_prompt(system_message=system_message, question=question)

        if classification in ["sql", "data_visualization", "insight"]:
            return classification
        else:
            return "unknown"

    def waii_sql_generator(self, question: str) -> str:
        try:
            query_id = str(uuid.uuid4())
            response = WAII.Query.generate(QueryGenerationRequest(uuid=query_id, ask=question))
            return response.query
        except Exception as e:
            print(f"Error generating query: {e}")
            return ""

    def waii_sql_executor(self, query: str) -> List[str]:
        try:
            response = WAII.Query.run(RunQueryRequest(query=query))
            print(f"Executed the query, num of rows: {len(response.rows)}")
            return response.rows
        except Exception as e:
            print(f"Error executing query: {e}")
            return []

    def waii_chart_generator(self, data: List[Dict[str, Any]]) -> str:
        try:
            df_data = pd.DataFrame(data)
            response = WAII.Chart.generate_chart(df=df_data)
            # TODO: Remove this later (may be dump the chart into some JPG?)
            print(f"Chart spec: {response.chart_spec}")
            return response.chart_spec
        except Exception as e:
            print(f"Error generating chart: {e}")
            raise e

    def waii_insight_generator(param: List[str]) -> str:
        # TODO: Need to integrate with WAII for generating query
        return "Insight: These are the top 5 directors."

    def run_workflow(self):
        while True:
            try:
                initial_state = State()
                app_response = self.app.invoke(initial_state)
                print(f"{app_response['response']}")
            except Exception as e:
                print(f"Error in workflow: {e}. Will restart.")


# Example usage
if __name__ == "__main__":
    # Who are the top 5 directors with the highest number of titles?
    workflow_manager = LanggraphWorkflowManager()
    workflow_manager.run_workflow()
