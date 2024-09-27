import sys
import uuid
from typing import List, Optional, Dict, Any

import pandas as pd
from langgraph.graph import StateGraph
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.schema import StrOutputParser
from pydantic import BaseModel
from waii_sdk_py import WAII
from waii_sdk_py.query import QueryGenerationRequest, RunQueryRequest

class State(BaseModel):
    database_description: str = ''
    query: str = ''
    sql: str = ''
    data: List[Dict[str, Any]] = []
    chart: str = ''
    insight: str = ''
    response: str = ''
    error: Optional[str] = None
    path_decision: str = ""

class LanggraphWorkflowManager:

    def __init__(self):
        self.workflow = StateGraph(State)
        self.workflow = self.create_workflow()
        self.app = self.workflow.compile()
        self.init_waii()
        print(f"Initialized Langgraph workflow:")
        print(self.app.get_graph().draw_ascii())

    def format_catalog_info(self, catalogs):
        formatted_info = []

        for catalog in catalogs.catalogs:
            catalog_name = catalog.name
            formatted_info.append(f"Database: {catalog_name}")

            for schema in catalog.schemas:
                schema_name = schema.name.schema_name
                schema_description = schema.description

                formatted_info.append(f"  Schema: {schema_name}")
                formatted_info.append(f"    Description: {schema_description}")

            formatted_info.append("")

        return "\n".join(formatted_info)

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
        workflow.add_node("Question Classifier", self.question_classifier)
        workflow.add_node("SQL Generator", self.sql_generator)
        workflow.add_node("SQL Executor", self.sql_executor)
        workflow.add_node("Chart Generator", self.chart_gen)
        workflow.add_node("Insight Generator", self.insight_generator)
        workflow.add_node("Result Synthesizer", self.result_synthesizer)

        # Define edges to control workflow execution
        workflow.set_entry_point("Question Classifier")
        workflow.add_conditional_edges(
            "Question Classifier",
            lambda state: state.path_decision,
            {
                "database": "SQL Generator",
                "visualization": "Chart Generator",
                "insight": "Insight Generator"
            }
        )
        workflow.add_edge("SQL Generator", "SQL Executor")
        workflow.add_edge("SQL Executor", "Result Synthesizer")
        workflow.add_edge("Chart Generator", "Result Synthesizer")
        workflow.add_edge("Insight Generator", "Result Synthesizer")

        # Loop through the workflow
        workflow.add_edge("Result Synthesizer", "Question Classifier")

        return workflow

    def get_user_input(self):
        query = input("Enter your question (Type 'exit' to quit): ")
        if query.lower() == "exit":
            print(f"Exiting...")
            sys.exit()
        return query

    def question_classifier(self, state: State) -> State:

        if not state.database_description:
            state.database_description = self.format_catalog_info(WAII.Database.get_catalogs())

        state.query = self.get_user_input()

        # Classify the question to one of sql, insight, data_visualization, or unknown
        question = self.waii_question_classification(query=state.query, database_description=state.database_description)

        if question in ["database", "insight", "visualization"]:
            return state.model_copy(update={"path_decision": question, "error": None})

    def sql_generator(self, state: State) -> State:
        print(f"Generating SQL for query: {state.query}")
        try:
            sql = self.waii_sql_generator(question=state.query)
            return state.model_copy(update={"sql": sql})
        except Exception as e:
            raise Exception(f"Error in SQL generation: {str(e)}")

    def sql_executor(self, state: State) -> State:
        print(f"Executing query: {state.query}")
        try:
            data = self.waii_sql_executor(query=state.sql)
            updated_state = state.model_copy(update={"data": data}, deep=True)
            print(f"State after exec: {updated_state}")
            return updated_state
        except Exception as e:
            raise Exception(f"Error in SQL generation: {str(e)}")

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
        print(f"Generating insight for data: {state.query}")
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

    def waii_question_classification(self, query: str, database_description: str) -> str:
        # Create the language model
        model = ChatOpenAI()

        # Create the chat prompt template
        prompt = ChatPromptTemplate.from_messages([
            ("system", "You are an expert in classifying questions into 'database', 'visualization', or 'insight'. Use 'database' if the question can be answered from the movie and tv database, 'visualization' if the user would be best served by a graph, 'insight' if it's a general question you can answer from memory. Prefer 'database' if multiple apply. Here is a description of what's in the database: '\n---\n{database_description}\n---\n'"),
            ("human", "Can you classify the following question into one of these categories? Question: '{query}'. "
             "Output: Strictly respond with either 'database', 'visualization', or 'insight'. No additional text.")
        ])

        # Create the chain
        chain = prompt | model | StrOutputParser()

        # Invoke the chain and get the classification
        classification = chain.invoke({"query": query, "database_description": database_description}).strip().lower()

        # Return the classification, mapping 'others' to 'unknown'
        if classification in ["database", "visualization", "insight"]:
            return classification
        else:
            return "insight"

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

            print(f"Chart spec: {response.chart_spec}")
            return response.chart_spec
        except Exception as e:
            print(f"Error generating chart: {e}")
            raise e

    def waii_insight_generator(self, query: str) -> str:
        # Create the language model
        model = ChatOpenAI()

        # Create the chat prompt template
        prompt = ChatPromptTemplate.from_messages([
            ("system", "You are an AI assistant that generates insightful responses to any query. Provide a concise, relevant insight based on the user's question."),
            ("human", "Please provide an insightful response to the following question: '{query}'. "
             "Your response should be informative and directly address the query.")
        ])

        # Create the chain
        chain = prompt | model | StrOutputParser()

        # Generate the insight
        insight = chain.invoke({"query": query})

        return insight.strip()

    def run_workflow(self):
        while True:
            try:
                initial_state = State()
                app_response = self.app.invoke(initial_state)
                print(f"{app_response['response']}")
            except Exception as e:
                print(f"Error in workflow: {e}. Will restart.")


if __name__ == "__main__":
    workflow_manager = LanggraphWorkflowManager()
    workflow_manager.run_workflow()
