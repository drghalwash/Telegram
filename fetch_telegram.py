import os
import asyncio
from telethon import TelegramClient, errors
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Telegram API credentials
api_id = int(os.getenv("TELEGRAM_API_ID"))
api_hash = os.getenv("TELEGRAM_API_HASH")
session_name = "telegram_session"

# Supabase credentials
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_ANON_KEY")

# Initialize Supabase client
supabase: Client = create_client(supabase_url, supabase_key)

# Channel username or ID
channel_username = "MasterTheMRCSPartA"  # Change this to the target channel


async def get_last_saved_message_id():
    """Fetch the latest saved message_id from `telegram_metadata`."""
    try:
        response = supabase.table("telegram_metadata").select("last_saved_message_id").order("id", desc=True).limit(1).execute()
        if response.data and response.data[0]["last_saved_message_id"]:
            last_id = response.data[0]["last_saved_message_id"]
            print(f"📌 Debug: Correct Last Saved ID = {last_id}")
            return last_id
        return None  # No messages exist
    except Exception as e:
        print(f"❌ Error fetching last saved message ID: {e}")
        return None


async def save_last_saved_message_id(message_id):
    """Save the last processed message_id in `telegram_metadata`."""
    try:
        supabase.table("telegram_metadata").upsert({"id": 1, "last_saved_message_id": message_id}).execute()
        print(f"💾 Progress saved: Last message_id = {message_id}")
    except Exception as e:
        print(f"❌ Error saving last message ID: {e}")


async def save_to_supabase(messages):
    """Save new messages to Supabase, avoiding duplicates."""
    try:
        message_ids = [msg.id for msg in messages]

        # Check existing messages in Supabase
        response = supabase.table("telegram_messages").select("message_id").in_("message_id", message_ids).execute()
        existing_ids = {row["message_id"] for row in response.data} if response.data else set()

        # Filter only new messages
        new_messages = [
            {
                "message_id": msg.id,
                "content": msg.text,
                "date": msg.date.isoformat(),
                "sender_id": msg.sender_id
            }
            for msg in messages if msg.id not in existing_ids and msg.text
        ]

        if new_messages:
            insert_response = supabase.table("telegram_messages").insert(new_messages).execute()
            if insert_response.data:
                print(f"✅ Saved {len(new_messages)} new messages to Supabase!")
                return len(new_messages), new_messages[-1]["message_id"]  # Return last saved ID
            else:
                print("❌ Error inserting new messages.")
                return 0, None
        else:
            print("🚫 No new messages to save.")
            return 0, None
    except Exception as e:
        print(f"❌ Error saving messages to Supabase: {e}")
        return 0, None


async def fetch_telegram_data():
    """Fetch and save messages from Telegram."""
    try:
        async with TelegramClient(session_name, api_id, api_hash) as client:
            print("✅ Connected to Telegram!")

            last_saved_id = await get_last_saved_message_id()

            if last_saved_id:
                print(f"🔄 Resuming from message_id: {last_saved_id}")
            else:
                print("🆕 No saved messages found, fetching from latest.")

            batch_size = 100  # Adjust batch size to avoid rate limits
            max_id = last_saved_id if last_saved_id else None  # Start from last saved ID

            while True:
                try:
                    # Fetch messages
                    messages = await client.get_messages(channel_username, limit=batch_size, max_id=max_id) if max_id else await client.get_messages(channel_username, limit=batch_size)
                    
                    if not messages:
                        print("✅ No more new messages found, stopping.")
                        break  # Stop if no messages left

                    print(f"📥 Fetched {len(messages)} messages, processing...")
                    saved_count, last_message_id = await save_to_supabase(messages)

                    if saved_count > 0:
                        await save_last_saved_message_id(last_message_id)  # Save the correct last message ID

                    if saved_count == 0:
                        print("🚫 No new messages to save, stopping.")
                        break  # Stop if nothing new is added

                    max_id = messages[-1].id - 1  # Move to next batch

                    # Sleep to prevent rate limit issues
                    await asyncio.sleep(2)

                except errors.FloodWaitError as e:
                    print(f"⏳ Rate limit hit! Waiting {e.seconds} seconds before retrying...")
                    await asyncio.sleep(e.seconds)

                except Exception as e:
                    print(f"❌ Unexpected error fetching messages: {e}")
                    break  # Exit on unexpected error

    except KeyboardInterrupt:
        print(f"\n🛑 Script interrupted manually. Saving progress at message_id {last_saved_id}...")
        await save_last_saved_message_id(last_saved_id)  # Save progress before exit

    except Exception as e:
        print(f"❌ Critical error: {e}")

    finally:
        print("🏁 All messages fetched and saved!")


# Run the script
asyncio.run(fetch_telegram_data())
