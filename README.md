# GSHackathonGroup3

## GoldSight - A Text Analysis Tool To Help Understand Your Users and Developers Better

### Pre - Requisites: 
Extract the Symphony chats using a scraper tool to get a .csv file

### Step 1: Connect with GoldSight API Gateway
curl --location --request POST 'https://1bo0ta8uk3.execute-api.ap-southeast-1.amazonaws.com/default/generate-insights-python' \
--form 'file=@<insert file location>'
  
### Step 2: Open the generated URL and view your Dashboard
