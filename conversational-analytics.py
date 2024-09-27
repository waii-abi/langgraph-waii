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

        workflow.add_node("Question Classifier", self.question_classifier)
        workflow.add_node("SQL Generator", self.sql_generator)
        workflow.add_node("SQL Executor", self.sql_executor)
        workflow.add_node("Chart Generator", self.chart_gen)
        workflow.add_node("Insight Generator", self.insight_generator)
        workflow.add_node("Result Synthesizer", self.result_synthesizer)

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

        workflow.add_edge("Result Synthesizer", "Question Classifier")

        return workflow

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

    def question_classifier(self, state: State) -> State:

        state.database_description = self.format_catalog_info(WAII.Database.get_catalogs())
        state.query = input("Enter your question: ")

        prompt = ChatPromptTemplate.from_messages([
            ("system", "You are an expert in classifying questions into 'database', 'visualization', or 'insight'. Use 'database' if the question can be answered from the movie and tv database, 'visualization' if the user would be best served by a graph, 'insight' if it's a general question you can answer from memory. Prefer 'database' if multiple apply. Here is a description of what's in the database: '\n---\n{database_description}\n---\n'"),
            ("human", "Can you classify the following question into one of these categories? Question: '{query}'. "
             "Output: Strictly respond with either 'database', 'visualization', or 'insight'. No additional text.")
        ])
        chain = prompt | ChatOpenAI() | StrOutputParser()
        classification = chain.invoke({"query": state.query, "database_description": state.database_description}).strip().lower()

        return state.model_copy(update={"path_decision": classification, "error": None})

    def sql_generator(self, state: State) -> State:
        sql = WAII.Query.generate(QueryGenerationRequest(ask=state.query)).query
        return state.model_copy(update={"sql": sql})

    def sql_executor(self, state: State) -> State:
        data = WAII.Query.run(RunQueryRequest(query=state.sql)).rows
        return state.model_copy(update={"data": data}, deep=True)

    def chart_gen(self, state: State) -> State:
        df_data = pd.DataFrame(state.data)
        chart = WAII.Chart.generate_chart(df=df_data).chart_spec
        return state.model_copy(update={"chart": str(chart), "error": None}, deep=True)

    def insight_generator(self, state: State) -> dict:
        prompt = ChatPromptTemplate.from_messages([("human", "{query}")])
        chain = prompt | ChatOpenAI() | StrOutputParser()
        insight = chain.invoke({"query": state.query})
        return state.model_copy(update={"insight": insight, "error": None}, deep=True)

    def format_data(self, data: List[Dict[str, Any]]) -> str:
        formatted_data = ""
        for row in data:
            formatted_data += " | ".join([f"{key}: {value}" for key, value in row.items()])
            formatted_data += "\n"
        return formatted_data

    def result_synthesizer(self, state: State) -> State:

        model = ChatOpenAI()
        prompt = ChatPromptTemplate.from_messages([
            ("system", "You are an expert assistant in analyzing data"),
            ("human", "\n User Question: '{query}'. "
                             "\n Results of query (if any): '{data}'."
                             "\n LLM results (if any): '{insight}'."
                             "\n\n Instructions: Answer the user with this information.")
        ])
        chain = prompt | model | StrOutputParser()
        output = chain.invoke({"query": state.query, "data": self.format_data(state.data), "insight": state.insight}).strip().lower()
        print(output)
        return state.model_copy(update={"response": output}, deep=True)

    def run_workflow(self):
        while True:
            try:
                initial_state = State()
                app_response = self.app.invoke(initial_state)
                print(f"{app_response['response']}")
            except Exception as e:
                print(f"Error in workflow: {e}. Will restart.")

LanggraphWorkflowManager().run_workflow()
