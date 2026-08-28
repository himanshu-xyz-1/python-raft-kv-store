import tkinter as tk
from dashboard.app import ClusterDashboard

def main():
    root = tk.Tk()
    app = ClusterDashboard(root)
    root.mainloop()

if __name__ == "__main__":
    main()
