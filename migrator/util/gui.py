import tkinter as tk
from tkinter import filedialog, simpledialog
from ..models.migration_type import MigrationType


def get_file_location():
    """Open a file dialog to select an Excel file."""
    root = tk.Tk()
    root.withdraw()  # Hide the main tkinter window
    file_path = filedialog.askopenfilename(
        title="Select QB Data Export Excel File",
        filetypes=[("Excel Files", "*.xlsx"), ("All Files", "*.*")],
    )
    return file_path

def get_dsn_name():
    """Prompt the user to enter the DSN name."""
    root = tk.Tk()
    root.withdraw()  # Hide the main tkinter window
    dsn_name = simpledialog.askstring("DSN", "Enter the target DSN name:")
    return dsn_name

def get_migration_type():
    """Display a Tkinter window with buttons for each migration type."""
    root = tk.Tk()
    root.title("Select Migration Type")

    selected_migration_type = tk.StringVar()

    def select_migration_type(migration_value):
        selected_migration_type.set(migration_value)  # Set the selected type
        root.quit()  # Quit the main loop after selecting

    # Create a button for each migration type
    for mt in MigrationType:
        tk.Button(
            root,
            text=mt.value,
            command=lambda mt_value=mt.value: select_migration_type(mt_value),
        ).pack(pady=5)

    root.mainloop()  # Run the window until a button is pressed
    root.destroy()  # Close the window

    return selected_migration_type.get()
