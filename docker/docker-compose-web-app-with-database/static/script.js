// Wait for the HTML document to be fully loaded.
document.addEventListener('DOMContentLoaded', () => {
    // Get references to the HTML elements.
    const userTableBody = document.getElementById('userTableBody');
    const userTable = document.getElementById('userTable');
    const loadingMessage = document.getElementById('loading');
    const addUserForm = document.getElementById('addUserForm');
    const usernameInput = document.getElementById('username');
    const emailInput = document.getElementById('email');

    // Function to fetch users from the API and display them.
    const fetchUsers = async () => {
        userTable.classList.add('hidden');
        loadingMessage.classList.remove('hidden');

        try {
            // Make a GET request to the /users endpoint.
            const response = await fetch('/users');
            if (!response.ok) {
                throw new Error(`HTTP error! Status: ${response.status}`);
            }
            const users = await response.json();
            
            // Clear the existing table body.
            userTableBody.innerHTML = '';

            if (users.length === 0) {
                // Display a message if no users are found.
                userTableBody.innerHTML = '<tr><td colspan="3" class="text-center text-gray-500 py-4">No users found.</td></tr>';
            } else {
                // Populate the table with user data.
                users.forEach(user => {
                    const row = document.createElement('tr');
                    row.className = 'border-b border-gray-200 last:border-0';
                    row.innerHTML = `
                        <td class="p-3">${user.id}</td>
                        <td class="p-3">${user.username}</td>
                        <td class="p-3">${user.email}</td>
                    `;
                    userTableBody.appendChild(row);
                });
            }
        } catch (error) {
            console.error('Error fetching users:', error);
            userTableBody.innerHTML = `<tr><td colspan="3" class="text-center text-red-500 py-4">Error loading users.</td></tr>`;
        } finally {
            loadingMessage.classList.add('hidden');
            userTable.classList.remove('hidden');
        }
    };

    // Function to handle the form submission.
    const handleFormSubmit = async (event) => {
        event.preventDefault(); // Prevent the default form submission.

        const newUserData = {
            username: usernameInput.value,
            email: emailInput.value
        };

        try {
            // Make a POST request to add a new user.
            const response = await fetch('/users', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(newUserData)
            });

            if (!response.ok) {
                throw new Error(`HTTP error! Status: ${response.status}`);
            }

            // Clear the form fields after successful submission.
            usernameInput.value = '';
            emailInput.value = '';
            
            // Refresh the user list to show the new user.
            fetchUsers();

        } catch (error) {
            console.error('Error adding user:', error);
            alert('Failed to add user. Please try again.');
        }
    };

    // Attach event listener to the form.
    addUserForm.addEventListener('submit', handleFormSubmit);

    // Fetch the users when the page first loads.
    fetchUsers();
});
