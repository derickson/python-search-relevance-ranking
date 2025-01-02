from dotenv import load_dotenv
from httpx import AsyncClient
from datetime import datetime, timezone
import streamlit as st
import asyncio
import json
import os

from pydantic_ai.messages import ModelResponse, TextPart

from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIModel

load_dotenv()


################
## AGENT
################

agent = Agent()
model = OpenAIModel('gpt-4o')

## The part that streams a response from the Agent

async def prompt_ai(messages):
    async with agent.run_stream(messages, model=model, message_history=st.session_state.messages) as result: 
        async for message in result.stream_text(delta=True):
            yield message
    # Add user message to chat history
    st.session_state.messages.append(result.new_messages()[-1]) 
        

      

###############
## The UI
###############

async def main():

    st.title("Star Wars Trivia")

    # Initialize chat history -- https://ai.pydantic.dev/api/messages/
    if "messages" not in st.session_state:
        st.session_state.messages = []    

    # Display chat messages from history on app rerun
    for message in st.session_state.messages:
        role = message.kind
        parts = message.parts
        content = "".join(part.content for part in message.parts)

        if role in ["request", "response"]:
            with st.chat_message("human" if role == "request" else "ai"):
                st.markdown(content)        

    # React to user input
    if prompt := st.chat_input("Ask a Star Wars trivia question:"):
        # Display user message in chat message container
        st.chat_message("user").markdown(prompt)
        

        # Display assistant response in chat message container
        response_content = ""
        with st.chat_message("ai"):
            message_placeholder = st.empty()  # Placeholder for updating the message
            # Run the async generator to fetch responses
            async for chunk in prompt_ai(prompt):
                
                response_content += chunk
                # Update the placeholder with the current response content
                message_placeholder.markdown(response_content)
        
        
        # Add response to chat history
        ai_response = ModelResponse(
            parts=[
                TextPart(
                    content=response_content, 
                    part_kind='text')
                ], 
            timestamp=datetime.now(timezone.utc),  # Current UTC time
            kind='response'
        )
        st.session_state.messages.append(ai_response) 


if __name__ == "__main__":
    asyncio.run(main())