import os
import logging
import io
import time
import traceback
from flask import Flask, request, jsonify, make_response, send_from_directory, redirect, url_for, send_file
import json
import pandas as pd
from openai import OpenAI, APIError
from dotenv import load_dotenv
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import cast, func
from sqlalchemy.types import Date
from datetime import datetime, timedelta
import random
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows




# Load environment variables
load_dotenv()

# Configure logging to use UTF-8 encoding
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', encoding='utf-8')
logger = logging.getLogger(__name__)



# Initialize OpenAI client
apikey=os.getenv('OPENAI_API_KEY')
client = OpenAI(api_key=apikey, organization=None)

# Print OpenAI version
import openai
logger.info(f"OpenAI version: {openai.__version__}")

app = Flask(__name__, static_folder='static')




@app.route('/')
def index():
    return send_from_directory('static', 'index.html')

@app.route('/chat')
def chat():
    return send_from_directory('static', 'chat.html')

@app.route('/upload')
def upload():
    return send_from_directory('static', 'upload.html')


@app.route('/api/get-today-thread', methods=['POST'])
def get_today_thread():
    user_id = request.json.get('user_id')
    if not user_id:
        return jsonify({"error": "User ID is required"}), 400
    
    today = datetime.utcnow().date()
    thread = Thread.query.filter(
        Thread.user_id == user_id,
        cast(Thread.created_at, Date) == today
    ).first()
    
    if thread:
        return jsonify({"thread_id": thread.thread_id, "assistant_id": thread.assistant_id})
    else:
        return jsonify({"error": "No thread found for today"}), 404
    


@app.route('/api/get-latest-thread', methods=['POST'])
def get_latest_thread():
    user_id = request.json.get('user_id')
    if not user_id:
        return jsonify({"error": "User ID is required"}), 400
    
    one_month_ago = datetime.utcnow() - timedelta(days=30)
    thread = Thread.query.filter(
        Thread.user_id == user_id,
        Thread.created_at >= one_month_ago
    ).order_by(Thread.created_at.desc()).first()
    
    if thread:
        return jsonify({"thread_id": thread.thread_id, "assistant_id": thread.assistant_id})
    else:
        return jsonify({"error": "No thread found within the last month"}), 404
    

@app.route('/api/get-or-create-thread', methods=['POST'])
def get_or_create_thread():
    user_id = request.json.get('user_id')
    if not user_id:
        return jsonify({"error": "User ID is required"}), 400
    
    today = datetime.utcnow().date()
    thread = Thread.query.filter(
        Thread.user_id == user_id,
        cast(Thread.created_at, Date) == today
    ).first()
    
    if thread:
        return jsonify({"thread_id": thread.thread_id, "assistant_id": thread.assistant_id})
    else:
        try:
            thread_id, assistant_id = create_new_thread(user_id)
            return jsonify({"thread_id": thread_id, "assistant_id": assistant_id})
        except Exception as error:
            db.session.rollback()
            logger.error(f"Error creating new thread: {str(error)}")
            return jsonify({"error": "Failed to create new thread", "details": str(error)}), 500
        

@app.route('/api/create-thread', methods=['POST'])
def create_thread():
    try:
        data = request.get_json(silent=True) or request.form
        user_id = data.get('user_id', 'default_user')
        assistant_id = get_or_create_assistant()
        thread = client.beta.threads.create()
        new_thread = Thread(thread_id=thread.id, user_id=user_id, assistant_id=assistant_id)
        db.session.add(new_thread)
        db.session.commit()
        logger.info(f"New thread created with ID: {thread.id} and assistant ID: {assistant_id}")
        return jsonify({"thread_id": thread.id, "assistant_id": assistant_id}), 200
    except Exception as error:
        db.session.rollback()
        logger.error(f'Error creating new thread: {str(error)}')
        return jsonify({"error": 'Failed to create thread', "details": str(error)}), 500
    

def create_new_thread(user_id):
    assistant_id = get_or_create_assistant()
    thread = client.beta.threads.create()
    new_thread = Thread(thread_id=thread.id, user_id=user_id, assistant_id=assistant_id)
    db.session.add(new_thread)
    db.session.commit()
    logger.info(f"New thread created with ID: {thread.id} and assistant ID: {assistant_id}")
    return thread.id, assistant_id

def get_or_create_assistant():
    today = datetime.utcnow().date()
    assistant = Assistant.query.filter(cast(Assistant.created_at, Date) == today).first()
    if assistant:
        return assistant.assistant_id
    else:
        new_assistant_id = create_assistant()
        new_assistant = Assistant(assistant_id=new_assistant_id, user_id='default_user')  # Add user_id here
        db.session.add(new_assistant)
        db.session.commit()
        return new_assistant_id

@app.route('/api/get-or-create-assistant', methods=['POST'])
def api_get_or_create_assistant():
    try:
        assistant_id = get_or_create_assistant()
        return jsonify({"assistant_id": assistant_id}), 200
    except Exception as error:
        logger.error(f'Error in get_or_create_assistant: {str(error)}')
        return jsonify({"error": 'Failed to get or create assistant', "details": str(error)}), 500
    

@app.route('/api/check-data-uploaded', methods=['POST'])
def check_data_uploaded():
    data = request.get_json(silent=True) or request.form
    thread_id = data.get('threadId')
    
    if not thread_id:
        return jsonify({"error": 'threadId is required'}), 400
    
    thread = Thread.query.filter_by(thread_id=thread_id).first()
    if not thread:
        return jsonify({"error": 'Thread not found'}), 404
    
    return jsonify({"dataUploaded": thread.data_uploaded}), 200
        
        
# Database configuration
app.config['SQLALCHEMY_DATABASE_URI'] = f"mssql+pyodbc://@localhost/{os.getenv('DB_NAME')}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
db = SQLAlchemy(app)

class Assistant(db.Model):
    __tablename__ = 'Assistants'
    assistant_id = db.Column(db.String(50), primary_key=True)
    user_id = db.Column(db.String(50), nullable=False, default='default_user')  # Add this line
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class Thread(db.Model):
    __tablename__ = 'Threads'
    thread_id = db.Column(db.String(50), primary_key=True)
    user_id = db.Column(db.String(50), nullable=False)
    assistant_id = db.Column(db.String(50))  # Make sure this line is present
    data_uploaded = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    conversations = db.relationship('Conversation', backref='thread', lazy=True)

class Conversation(db.Model):
    __tablename__ = 'Conversations'
    conversation_id = db.Column(db.Integer, primary_key=True)
    thread_id = db.Column(db.String(50), db.ForeignKey('Threads.thread_id'), nullable=False)
    user_id = db.Column(db.String(50), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

with app.app_context():
    db.create_all()

ASSISTANT_ID = None

def create_assistant():
    try:
        assistant = client.beta.assistants.create(
            name='Financial Assistant',
            instructions="""
            You are a financial assistant. You are expected to answer questions based on the provided financial data. The user will ask you to answer questions in these possible formats: Simple Response (String), Excel file, table. Responses should be given in one of three formats: string, table, or excel. 
            Always default to the string format unless the user specifies a different format in the message.
            You will only provide the response in the requested format. For each format, the response should be structured as follows:
            For the string format, the JSON structure should be:
            {
            "response_type": "string",
            "data": {
            "comments": "Any additional comments or remarks."
            },
            }
            For the excel format, always return the data, never return any file path or anything. The JSON structure should be:
            {
            "status": "success",
            "response_type": "excel",
            "data": {
            "headers": ["Question", "Answer", "Total Revenue", "Expenses", "Profit", "Comments"],
            "rows": [
            ["Your question here", "The answer here", "Total revenue value", "Expenses value", "Profit value", "Any additional comments"]
            ]
            },
            }
            For the table format, the JSON structure should be:
            {
            "status": "success",
            "response_type": "table",
            "data": {
            "headers": ["Question", "Answer", "Total Revenue", "Expenses", "Profit", "Comments"],
            "rows": [
            ["Your question here", "The answer here", "Total revenue value", "Expenses value", "Profit value", "Any additional comments"]
            ]
            },
            }
            You will answer only in the specified format (string, table, or excel) and include the "response_type" attribute in the response to indicate the format used. The headers in excel file and table are just given as an example for you to understand. So, they need to be adjusted according to the financial data headers provided to you later in this conversation. So, use them as headers for both excel and table. Do not provide all formats in one response; answer based on the type requested. Financial data will be provided in a through JSON. The answers should reflect this data.
            If user asks you to return a file or anything, just return the data in the format specified above, and the user will handle the rest. DO NOT return any file path EVER.
            """,
            model='gpt-4o-mini'
        )
        return assistant.id
    except Exception as error:
        logger.error(f'Error initializing assistant: {str(error)}')
        raise

def create_new_thread(user_id):
    assistant_id = create_assistant()
    thread = client.beta.threads.create()
    new_thread = Thread(thread_id=thread.id, user_id=user_id, assistant_id=assistant_id)
    db.session.add(new_thread)
    db.session.commit()
    logger.info(f"New thread created with ID: {thread.id} and assistant ID: {assistant_id}")
    return thread.id, assistant_id

@app.route('/api/init-assistant', methods=['POST'])
def init_assistant():
    """
    Initialize a new Financial Assistant using OpenAI's beta API.
    """
    try:
        assistant_id = create_assistant()
        return jsonify({"assistantId": assistant_id}), 200
    except Exception as error:
        app.logger.error(f'Error initializing assistant: {str(error)}')
        return jsonify({"error": 'Failed to create assistant', "details": str(error)}), 500

@app.route('/api/init-thread', methods=['POST'])
def init_thread():
    try:
        data = request.get_json(silent=True) or request.form
        user_id = data.get('user_id', 'default_user')
        thread_id, assistant_id = create_new_thread(user_id)
        return jsonify({"threadId": thread_id, "assistantId": assistant_id}), 200
    except Exception as error:
        logger.error(f'Error in init_thread: {str(error)}')
        return jsonify({"error": 'Failed to create thread', "details": str(error)}), 500
    

@app.route('/api/list-messages', methods=['POST'])
def list_messages():
    try:
        data = request.get_json(silent=True) or request.form
        thread_id = data.get('threadId')
        
        if not thread_id:
            return jsonify({"error": 'threadId is required'}), 400
        
        messages = client.beta.threads.messages.list(thread_id=thread_id)
        
        message_list = []
        for message in messages.data:
            message_content = message.content[0].text.value if message.content else ""
            # Filter out data chunk messages
            if not message_content.startswith("Financial data chunk:"):
                message_list.append({
                    "id": message.id,
                    "role": message.role,
                    "content": message_content,
                    "created_at": message.created_at
                })
        
        return jsonify({"messages": message_list}), 200
    except Exception as error:
        logger.error(f'Error in list_messages: {str(error)}')
        return jsonify({"error": 'Failed to list messages', "details": str(error)}), 500
    
@app.route('/api/feed-data', methods=['POST'])
def feed_data():
    thread_id = request.form.get('threadId')
    if not thread_id:
        return jsonify({"error": 'threadId is required'}), 400
    
    files = request.files.getlist('files[]')
    if not files:
        return jsonify({"error": 'At least one Excel file is required'}), 400
    
    try:
        thread = Thread.query.filter_by(thread_id=thread_id).first()
        if not thread:
            return jsonify({"error": 'Thread not found'}), 404

        # Define maximum number of rows per chunk (adjust as needed)
        MAX_ROWS_PER_CHUNK = 250

        total_chunks = 0
        uploaded_chunks = 0

        for file in files:
            df = pd.read_excel(file)
            df.reset_index(inplace=True)
            df.rename(columns={'index': 'row_number'}, inplace=True)
            df['row_number'] += 1  # Increment row_number to start from 1
            data_json = df.to_dict(orient='records')
            
            # Split the data into chunks while maintaining structure
            chunks = []
            for i in range(0, len(data_json), MAX_ROWS_PER_CHUNK):
                chunk = {
                    "columns": df.columns.tolist(),
                    "rows": data_json[i:i + MAX_ROWS_PER_CHUNK]
                }
                chunks.append(chunk)
            
            total_chunks += len(chunks)

            # Attach each chunk to the thread as JSON
            for chunk in chunks:
                max_retries = 5
                attempt = 0
                while attempt < max_retries:
                    try:
                        client.beta.threads.messages.create(
                            thread_id=thread_id,
                            role="user",
                            content=f"Financial data chunk:\n{json.dumps(chunk)}"
                        )
                        uploaded_chunks += 1
                        logger.info(f"Successfully uploaded chunk {uploaded_chunks} out of {total_chunks} to thread {thread_id}.")
                        
                        # Add a delay between API calls (adjust as needed)
                        time.sleep(1)  # 1 second delay
                        
                        break  # Success, move to next chunk
                    except APIError as e:
                        if "rate_limit" in str(e).lower():
                            attempt += 1
                            if attempt == max_retries:
                                return jsonify({"error": f"Rate limit exceeded after {max_retries} attempts. Please try again later."}), 429
                            
                            wait_time = exponential_backoff(attempt)
                            logger.warning(f"Rate limit hit. Retrying in {wait_time:.2f} seconds...")
                            time.sleep(wait_time)
                        else:
                            raise  # Re-raise if it's not a rate limit error
                    except Exception as e:
                        logger.error(f"Error uploading chunk: {str(e)}")
                        return jsonify({"error": f"An error occurred while uploading data: {str(e)}"}), 500

        thread.data_uploaded = True
        db.session.commit()
        
        return jsonify({"message": f"Data uploaded successfully. {uploaded_chunks} chunks uploaded."}), 200
    except Exception as error:
        db.session.rollback()
        logger.error(f'Error uploading data: {str(error)}')
        return jsonify({"error": 'Failed to upload data', "details": str(error)}), 500
    

def exponential_backoff(attempt):
    return min(60, (2 ** attempt) + random.random())

@app.route('/api/get-response', methods=['POST'])
def get_response():
    max_retries = 5
    attempt = 0

    while attempt < max_retries:
        try:
            data = request.get_json(silent=True) or request.form
            
            thread_id = data.get('threadId')
            assistant_id = data.get('assistantId')
            question = data.get('question')
            user_id = data.get('user_id', 'default_user')
            
            if not thread_id or not assistant_id or not question:
                error_msg = 'threadId, assistantId, and question are required'
                logger.error(f"Invalid request: {error_msg}")
                return jsonify({"error": error_msg}), 400
            
            logger.info(f"Processing request for thread_id: {thread_id}, assistant_id: {assistant_id}")

            # Check if a conversation for this thread exists today
            today = datetime.utcnow().date()
            existing_conversation = Conversation.query.filter(
                Conversation.thread_id == thread_id,
                cast(Conversation.created_at, Date) == today
            ).first()
            
            if not existing_conversation:
                # Create a new conversation only if one doesn't exist for today
                new_conversation = Conversation(thread_id=thread_id, user_id=user_id)
                db.session.add(new_conversation)
                db.session.commit()
                logger.info(f"New conversation added to database: {new_conversation.conversation_id}")
            
            message = client.beta.threads.messages.create(
                thread_id=thread_id,
                role='user',
                content=question
            )
            logger.info(f"Message created: {message.id}")
            
            run = client.beta.threads.runs.create(
                thread_id=thread_id,
                assistant_id=assistant_id,
                model="gpt-4o-mini",
                temperature=0.1
            )
            logger.info(f"Run created: {run.id}")
            
            timeout = 300  # 5 minutes timeout
            start_time = time.time()
            while time.time() - start_time < timeout:
                time.sleep(5)  # Wait for 5 seconds before checking again
                run_status = client.beta.threads.runs.retrieve(thread_id=thread_id, run_id=run.id)
                logger.info(f"Run status: {run_status.status}")
                if run_status.status == 'completed':
                    break
                elif run_status.status in ['failed', 'cancelled', 'expired']:
                    error_msg = f"Run failed with status: {run_status.status}"
                    logger.error(f"{error_msg}\nFull run details: {run_status}")
                    return jsonify({
                        "error": error_msg,
                        "details": str(run_status)
                    }), 500
            else:
                logger.error("Request timed out")
                return jsonify({"error": "Request timed out"}), 504
            
            messages = client.beta.threads.messages.list(thread_id)
            for message in messages.data:
                if message.role == 'assistant':
                    response_content = message.content[0].text.value
                    logger.info(f"Raw assistant response: {response_content}")
                    try:
                        parsed_response = json.loads(response_content)
                        response_type = parsed_response.get('response_type', 'string')
                        logger.info(f"Parsed response type: {response_type}")
                        
                        if response_type in ['string', 'table', 'excel']:
                            return jsonify(parsed_response)
                        else:
                            return jsonify({"error": "Invalid response type"})

                    except json.JSONDecodeError:
                        logger.error("Invalid JSON response from assistant")
                        return jsonify({"error": "Invalid response from assistant"}), 500
                    except Exception as error:
                        logger.error(f"Error in get_response: {str(error)}")
                        return jsonify({"error": "An unexpected error occurred"}), 500

            logger.error("No assistant response found")
            return jsonify({"error": "No response from assistant"}), 500


        except APIError as api_error:
            logger.error(f"API error: {str(api_error)}")
            if "rate_limit" in str(api_error).lower():
                logger.warning(f"Rate limit error on attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    sleep_time = exponential_backoff(attempt)
                    logger.info(f"Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                    attempt += 1
                else:
                    logger.error("Max retries reached for rate limit error")
                    return jsonify({
                        "error": "Rate limit exceeded",
                        "message": "Please try again later or contact support."
                    }), 429
            else:
                return jsonify({
                    "error": "API error",
                    "message": str(api_error)
                }), 500
        except Exception as error:
            db.session.rollback()
            error_msg = f'Error in get_response: {str(error)}'
            logger.error(error_msg)
            logger.error(f"Full traceback:\n{traceback.format_exc()}")
            return jsonify({
                "error": 'Failed to get response',
                "details": error_msg,
                "traceback": traceback.format_exc()
            }), 500

    # If we've exhausted all retries
    return jsonify({
        "error": "Maximum retries reached",
        "message": "Unable to process request after multiple attempts. Please try again later."
    }), 500




@app.route('/api/get-excel', methods=['POST'])
def get_excel():
    try:
        data = request.json
        excel_file = json_to_excel(data)
        
        if excel_file:
            return send_file(
                io.BytesIO(excel_file),
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name='output.xlsx'
            )
        else:
            return jsonify({"error": "Failed to generate Excel file"}), 500
    except Exception as error:
        logger.error(f"Error generating Excel file: {str(error)}")
        return jsonify({"error": "An unexpected error occurred"}), 500

def json_to_excel(json_data):
    try:
        headers = json_data.get('headers', [])
        rows = json_data.get('rows', [])
        
        logger.info(f"Headers: {headers}")
        logger.info(f"First row: {rows[0] if rows else 'No rows'}")
        
        df = pd.DataFrame(rows, columns=headers)
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Sheet1', index=False)
            workbook = writer.book
            worksheet = writer.sheets['Sheet1']
            
            for i, col in enumerate(df.columns):
                max_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
                worksheet.set_column(i, i, max_len)
        
        output.seek(0)
        return output.getvalue()
    except Exception as e:
        logger.error(f"Error in json_to_excel: {str(e)}")
        logger.error(f"JSON data: {json_data}")
        return None


@app.route('/<path:filename>')
def serve_static(filename):
    return send_from_directory(app.static_folder, filename)


if __name__ == '__main__':
    port = int(os.getenv('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=True)