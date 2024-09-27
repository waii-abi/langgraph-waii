import os
import sys
from typing import List, Optional, Dict, Any

import pandas as pd
import plotly
from pydantic import BaseModel
from langgraph.graph import StateGraph
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.schema import StrOutputParser
from waii_sdk_py import WAII
from waii_sdk_py.query import QueryGenerationRequest, RunQueryRequest

class State(BaseModel):
    database_description: str = ''
    query: str = ''
    sql: str = ''
    data: List[Dict[str, Any]] = []
    chart: Any = ''
    insight: str = ''
    response: str = ''
    error: Optional[str] = None
    path_decision: str = ""

class LanggraphWorkflowManager:

    def init_waii(self):
        WAII.initialize(url=os.getenv("WAII_URL"), api_key=os.getenv("WAII_API_KEY"))
        WAII.Database.activate_connection(os.getenv("DB_CONNECTION"))

    def create_workflow(self) -> StateGraph:
        workflow = StateGraph(State)

        workflow.add_node("Question Classifier", self.question_classifier)
        workflow.add_node("Result Classifier", self.result_classifier)
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
                "general": "Insight Generator"
            }
        )

        workflow.add_edge("SQL Generator", "SQL Executor")
        workflow.add_edge("SQL Executor", "Result Classifier")
        workflow.add_conditional_edges(
            "Result Classifier",
            lambda state: state.path_decision,
            {
                "visualization": "Chart Generator",
                "data": "Result Synthesizer"
            }
        )
        workflow.add_edge("Chart Generator", "Result Synthesizer")
        workflow.add_edge("Insight Generator", "Result Synthesizer")
        workflow.add_edge("Result Synthesizer", "Question Classifier")

        return workflow

    def question_classifier(self, state: State) -> State:
        state.database_description = self.format_catalog_info(WAII.Database.get_catalogs())
        state.query = input("Question: ")

        prompt = ChatPromptTemplate.from_messages([
            ("human",
             "Database info: \n---\n{database_description}\n---\n"
             "Answer 'database' if this question is likely related to information in the database. Otherwise answer 'general'? Question: '{query}'. "
             "Consider the information you have about the database, when in doubt answer 'database'")
        ])
        chain = prompt | ChatOpenAI() | StrOutputParser()
        classification = chain.invoke({"query": state.query, "database_description": state.database_description}).strip().lower()
        return state.model_copy(update={"path_decision": classification, "error": None})

    def sql_generator(self, state: State) -> State:
        sql = WAII.Query.generate(QueryGenerationRequest(ask=state.query)).query
        return state.model_copy(update={"sql": sql, "insight":""})

    def sql_executor(self, state: State) -> State:
        data = WAII.Query.run(RunQueryRequest(query=state.sql)).rows
        return state.model_copy(update={"data": data}, deep=True)

    def chart_gen(self, state: State) -> State:
        df_data = pd.DataFrame(state.data)
        chart = WAII.Chart.generate_chart(df=df_data)
        return state.model_copy(update={"chart": chart.chart_spec, "error": None}, deep=True)

    def result_classifier(self, state: State) -> State:
        state.chart = ''
        prompt = ChatPromptTemplate.from_messages([
            ("human",
             "Is the following question best answered by 'data' or a 'visualization'? Question: '{query}'. "
             "Output: Strictly respond with either 'data', or 'visualization'. No additional text.")
        ])
        chain = prompt | ChatOpenAI() | StrOutputParser()
        classification = chain.invoke({"query": state.query}).strip().lower()
        return state.model_copy(update={"path_decision": classification, "error": None})

    def insight_generator(self, state: State) -> dict:
        prompt = ChatPromptTemplate.from_messages([("human", "{query}")])
        chain = prompt | ChatOpenAI() | StrOutputParser()
        insight = chain.invoke({"query": state.query})
        return state.model_copy(update={"insight": insight, "sql": "", "data": [], "error": None}, deep=True)

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
        data = "\n".join(" | ".join(f"{key}: {value}" for key, value in row.items()) for row in state.data)
        output = chain.invoke({"query": state.query, "data": data, "insight": state.insight}).strip().lower()
        if state.chart:
            df = pd.DataFrame(state.data)
            exec(state.chart.plot)
        print('Answer: '+output)
        return state.model_copy(update={"response": output}, deep=True)

    def __init__(self):
        self.workflow = self.create_workflow()
        self.app = self.workflow.compile()
        self.init_waii()
        print(self.app.get_graph().draw_ascii())

    def format_catalog_info(self, catalogs):
        return "\n".join([
            f"Database: {catalog.name}\n" +
            "\n".join([
                f"  Schema: {schema.name.schema_name}\n    Description: {schema.description}"
                for schema in catalog.schemas
            ]) + "\n"
            for catalog in catalogs.catalogs
        ])

    def run_workflow(self):
        while True:
            try:
                initial_state = State()
                app_response = self.app.invoke(initial_state)
            except Exception as e:
                print(f"Error in workflow: {e}. Will restart.")

LanggraphWorkflowManager().run_workflow()
