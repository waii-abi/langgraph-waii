# Using Waii with LangGraph

This sample application shows how to use Waii with LangGraph.

You will need a Waii API key, which you can request on www.waii.ai

## Setup Steps

1. **Set Environment Variables**

   Before running the script, you need to set up the following environment variables:

   - `WAII_URL`: The URL for the WAII service you want to use (default: sql.waii.ai)
   - `WAII_API_KEY`: Your API key for the WAII service
   - `DB_CONNECTION`: Your WAII database connection string

2. **Install Requirements**

   In the main folder run:

   ```bash
   pip install -r requirements.txt
   ```

   This will install all the necessary dependencies for the script.

3. **Run the Script**

   Once the environment variables are set and the requirements are installed, you can run the script:

   ```bash
   python conversational_analytics.py
   ```
