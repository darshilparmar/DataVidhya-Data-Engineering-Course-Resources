file_path = "script_to_load_EventStream.py"

# Step 1: Read file content
with open(file_path, "r") as file:
    content = file.read()

# Step 2: Convert to uppercase
upper_content = content.upper()

# Step 3: Write back to same file
with open(file_path, "w") as file:
    file.write(upper_content)

print("File updated successfully!")

