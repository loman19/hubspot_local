require('dotenv').config();
const express = require('express');

// Import the HubSpot workflow actions
const { main: fetchRegistrationsMain } = require('./workflow-action_2');
const { main: processBatchAMain } = require('./3a');
const { main: processBatchBMain } = require('./3b');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware to parse JSON bodies
app.use(express.json());

// Health check endpoint
app.get('/', (req, res) => {
    res.json({
        status: 'HubSpot Local Webhook Server Running',
        workflow: 'Fetch Event Updates and Registrations'
    });
});

// Main webhook endpoint to receive data from HubSpot
app.post('/webhook', async (req, res) => {
    try {
        console.log('\n=== Webhook Received ===');
        console.log('Time:', new Date().toISOString());

        // Extract input properties from HubSpot webhook
        const inputFields = req.body.inputFields || req.body;

        console.log('Event ID:', inputFields.hs_object_id);
        console.log('Status:', inputFields.cs_processing_queue_status);

        // Validate required input fields
        if (!inputFields.hs_object_id) {
            throw new Error('Missing required field: hs_object_id');
        }

        if (!inputFields.cs_event_id) {
            throw new Error('Missing required field: cs_event_id');
        }

        // Create the event object in HubSpot workflow format
        const event = {
            inputFields: inputFields,
            objectId: inputFields.hs_object_id
        };

        // Create callback function to capture the workflow response
        let workflowResponse = null;
        const callback = (response) => {
            workflowResponse = response;
        };

        // Route to the appropriate workflow action based on status
        const status = inputFields.cs_processing_queue_status;
        console.log(`Routing decision - Status: "${status}"`);

        if (status === '03_REG_BATCH_A') {
            console.log('✓ Routing to 3a.js (Process Batch A)');
            await processBatchAMain(event, callback);
        } else if (status === '03_REG_BATCH_B') {
            console.log('✓ Routing to 3b.js (Process Batch B)');
            await processBatchBMain(event, callback);
        } else {
            console.log('✓ Routing to workflow-action_2.js (Fetch Registrations)');
            await fetchRegistrationsMain(event, callback);
        }

        // Check if we got a response
        if (!workflowResponse) {
            throw new Error('Workflow action did not return a response');
        }

        // Extract output fields
        const outputFields = workflowResponse.outputFields || {};

        console.log('Result Status:', outputFields.cs_processing_queue_status);
        console.log('✓ Webhook processed successfully\n');

        // Optionally update HubSpot event directly if enabled
        if (process.env.UPDATE_HUBSPOT_DIRECTLY === 'true' && inputFields.hs_object_id) {
            console.log('\n=== Updating HubSpot Event Directly ===');
            await updateHubSpotEvent(inputFields.hs_object_id, outputFields);
        }

        // Respond to HubSpot with the output fields
        res.status(200).json({
            outputFields: outputFields
        });

    } catch (error) {
        console.error('\n=== Error Processing Webhook ===');
        console.error('Error:', error.message);
        console.error('Stack:', error.stack);

        res.status(500).json({
            error: error.message,
            outputFields: {
                cs_processing_queue_status: '99_WFL_ERROR',
                cs_processing_queue_data: JSON.stringify({
                    error: error.message,
                    timestamp: new Date().toISOString()
                })
            }
        });
    }
});

// Function to update HubSpot event properties via API
async function updateHubSpotEvent(eventId, outputFields) {
    const hubspot = require('@hubspot/api-client');

    try {
        const hubspotClient = new hubspot.Client({
            accessToken: process.env.HUBSPOT_ACCESS_TOKEN
        });

        const properties = {
            cs_processing_queue_status: outputFields.cs_processing_queue_status,
            cs_processing_queue_data: outputFields.cs_processing_queue_data
        };

        console.log(`Updating event ${eventId} with properties:`, properties);

        await hubspotClient.crm.objects.basicApi.update(
            process.env.HUBSPOT_EVENT_OBJECT_ID,
            eventId,
            { properties }
        );

        console.log('✓ Event properties updated successfully in HubSpot');
    } catch (error) {
        console.error('✗ Error updating HubSpot event:', error.message);
        throw error;
    }
}

// Start the server
app.listen(PORT, () => {
    console.log(`✓ Server running on http://localhost:${PORT}`);
    console.log(`✓ Webhook: http://localhost:${PORT}/webhook`);
});