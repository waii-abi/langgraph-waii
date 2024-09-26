import openai


def run_prompt(system_message: str, question: str) -> str:
    response = openai.chat.completions.create(
        model="gpt-4-turbo",
        messages=[
            {"role": "system", "content": system_message},
            {"role": "user", "content": question},
        ],
        temperature=0.0
    )

    try:
        content = response.choices[0].message.content.strip().lower()
        return content
    except Exception as e:
        print(f"Error in getting response: {e}")
        return ""