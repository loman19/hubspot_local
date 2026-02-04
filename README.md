# HubSpot Local Development Setup

This project allows you to run HubSpot custom code actions locally for development and testing.

## Prerequisites

- Node.js installed
- ngrok (for exposing local server to HubSpot)
- HubSpot account with API access

## Installation

```bash
npm install
```

## Configuration

1. Copy `.env` file and update the values:
   - `HUBSPOT_API_KEY`: Your HubSpot private app access token
   - `HUBSPOT_OBJECT_TYPE`: The object type you're working with (contacts, companies, deals, etc.)
   - `UPDATE_HUBSPOT_DIRECTLY`: Set to `true` if you want to update HubSpot via API, `false` to only return values

2. Get your HubSpot API Key:
   - Go to Settings → Integrations → Private Apps
   - Create a new private app or use an existing one
   - Copy the access token

## Running Locally

1. Start the server:
```bash
node index.js
```

2. In a new terminal, expose your local server using ngrok:
```bash
ngrok http 3000
```

3. Copy the ngrok HTTPS URL (e.g., `https://abc123.ngrok.io`)

4. In HubSpot:
   - Go to your workflow
   - Add a "Custom code" action
   - Set the webhook URL to: `https://abc123.ngrok.io/webhook`
   - Configure your input properties
   - Configure your output properties (output_property_1, output_property_2)

## How It Works

### Input from HubSpot
HubSpot sends data in this format:
```json
{
  "inputFields": {
    "property_name_1": "value1",
    "property_name_2": "value2",
    "hs_object_id": "12345"
  }
}
```

### Output to HubSpot
Your server responds with:
```json
{
  "outputFields": {
    "output_property_1": "result1",
    "output_property_2": "result2"
  }
}
```

### Direct HubSpot Updates (Optional)
If `UPDATE_HUBSPOT_DIRECTLY=true`, the server will also update HubSpot properties via the API.

## Customization

Edit the `processHubSpotData()` function in `index.js` to implement your custom logic:

```javascript
async function processHubSpotData(inputFields) {
  // Your custom logic here
  // Make API calls, process data, etc.
  
  return {
    result1: "your result 1",
    result2: "your result 2"
  };
}
```

## Testing

You can test the webhook locally using curl:

```bash
curl -X POST http://localhost:3000/webhook \
  -H "Content-Type: application/json" \
  -d '{
    "inputFields": {
      "property_name_1": "test value",
      "hs_object_id": "12345"
    }
  }'
```

## Troubleshooting

- **Server not starting**: Check if port 3000 is already in use
- **HubSpot not receiving data**: Verify ngrok is running and URL is correct
- **API errors**: Check your HUBSPOT_API_KEY is valid and has proper scopes
- **Properties not updating**: Ensure property names match exactly in HubSpot

## Environment Variables Reference

| Variable | Required | Description |
|----------|----------|-------------|
| `PORT` | No | Server port (default: 3000) |
| `HUBSPOT_API_KEY` | Yes* | HubSpot private app access token |
| `HUBSPOT_OBJECT_TYPE` | No | Object type to update (default: contacts) |
| `UPDATE_HUBSPOT_DIRECTLY` | No | Whether to update HubSpot via API (default: false) |

*Required only if `UPDATE_HUBSPOT_DIRECTLY=true`
