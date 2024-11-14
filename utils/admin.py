import questionary
from rethinkdb import r
conn = r.connect()

while True:
    choices = ["Initialise DB", "Add secret", "Exit"]
    selection = questionary.select("What would you like to do?", choices=choices).ask()
    if selection == "Initialise DB":
        confirm = questionary.confirm("Initialising the DB only works if the DB does not exist.\nIf the DB is partially initialised or broken, back up any data if necessary and delete it.\nInitialising the DB will *NOT* wipe any data on its own.\nContinue?", default=False).ask()
        if not confirm:
            print("Backing out.")
            continue
        print("You said", confirm)
        print("Initialising database...")
        import credb
        print("You will probably want to add a secret now.")
        choices.remove("Initialise DB")
        continue
    if selection == "Add secret":
        thing = {}
        thing['web'] = questionary.confirm("Is this a log client? (Log clients receive events as they occur, such as items being started, completed, etc, and log lines for the items, and have no other priveleges.)", default=False).ask()
        if not thing['web']:
            thing['kick'] = questionary.confirm("Would you like clients bearing this secret to be kicked with a message of your choice when they connect?", default=False).ask()
            if thing['kick']:
                while not thing.get("Kreason"):
                    thing['Kreason'] = questionary.text("Please provide the message that should be shown to them: ").ask()
        while not thing.get("fo"):
            thing['fo'] = questionary.text("Please provide a short note about the secret, for your own purposes: ").ask()
        print("Secret details:", thing)
        if not questionary.confirm("Add to database?", default=True).ask():
            continue
        i = r.db("twitch").table("secrets").insert(thing).run(conn)['generated_keys'][0]
        print("Done! Your secret is:", i)
        continue
    if selection == "Exit" or not selection:
        print("Goodbye!")
        break
