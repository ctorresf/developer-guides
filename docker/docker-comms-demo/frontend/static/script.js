// Wait for the HTML document to be fully loaded before running the script.
document.addEventListener('DOMContentLoaded', () => {
    // Get a reference to the button and the message display area.
    const fetchButton = document.getElementById('fetch-button');
    const messageDisplay = document.getElementById('message');

    // Add a click event listener to the button.
    fetchButton.addEventListener('click', async () => {
        // Update the display to show a loading state.
        messageDisplay.textContent = 'Fetching data...';

        try {
            // Make an asynchronous request to the Flask API endpoint.
            const response = await fetch('/api/data');
            
            // Check if the network response was successful.
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            // Parse the JSON data from the response.
            const data = await response.json();

            // Update the message display with the data from the backend.
            messageDisplay.textContent = data.message;
        } catch (error) {
            // Handle any errors that occur during the fetch process.
            console.error('There was a problem with the fetch operation:', error);
            messageDisplay.textContent = 'Failed to fetch data. Check the console for details.';
        }
    });
});
