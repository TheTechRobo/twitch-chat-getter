function genTop(item, id, size, req, dateStarted, author, explain, dateClaimed) {
	let top = document.createElement("div");
	top.id = `top-${id}`;
	let isoString = new Date(dateStarted).toISOString().slice(0, 10);
	let claimedString = new Date(dateClaimed).toISOString().slice(0, 10);
	let prettyTS = `queued <abbr title="${new Date(dateStarted).toISOString()}">${isoString}</abbr>`;
	let prettyClaimed = `claimed <abbr title="${new Date(dateClaimed).toISOString()}">${claimedString}</abbr>`;
	let prettyMiB = `${(size/1024/1024).toFixed(1)} MiB`;
	let prettyReqs = `${req} requests`;
	let dataToAdd = {item: item, author: `for ${author}`, dateStarted: prettyTS, dateClaimed: prettyClaimed, id: id, explain: explain};
	Object.keys(dataToAdd).forEach((i) => {
		let item = document.createElement("div");
		item.classList.add("pure-u-1-6");
		item.innerHTML = dataToAdd[i];
		top.appendChild(item);
	});
	return top;
}

current_cards = {};

function createJobCard(item, id, ts, author, explain, ctask, dateClaimed) {
	let card = document.createElement("div");
	card.id = `card-${id}`;
	card.classList.add("card");

	let top = genTop(item, id, 0, 0, ts, author, explain, dateClaimed);
	card.appendChild(top);
	let log = document.createElement("div");
	log.id = `log-${id}`;
	log.classList.add("pure-u-1");
	log.classList.add("log");
	card.appendChild(log);

	let tsMap = document.createElement("div");
	tsMap.classList.add("ts-map");
	tsMap.innerHTML = `Current task: ${ctask}`;
	card.appendChild(tsMap);

	log.onmouseover = (e) => { log.nothing = true; log.classList.add("shadow") };
	log.onmouseleave = (e) => { log.nothing = false; log.classList.remove("shadow"); };

	current_cards[id] = `card-${id}`;

	document.getElementById("th").prepend(card);
}

function addLogEntry(id, entry) {
	if (current_cards[id] === undefined) {
		throw new Error("uh oh");
	}
	let opt = new Option(entry);
	let log = document.getElementById(current_cards[id]).querySelector(".log");
	log.appendChild(opt);
	if (!log.nothing) {
		log.scroll(0, 2100000)
	}
}

function makeWS() {
	let ENDPOINT = "ws://192.168.2.248:6969";
	let sock = new WebSocket(ENDPOINT);
	sock.addEventListener("open", (event) => {
		sock.send(JSON.stringify({type: "subscribe", auth: "hunter2"}));
	});
	sock.addEventListener("error", (event) => {
		console.log(event);
		makeWS();
	});
	sock.addEventListener("message", (event) => {
		let msg = JSON.parse(event.data);
		if (msg.type == "godot") {
			//sock.send(JSON.stringify({type: "ping"}));
		} else if (msg.type == "WLOG") {
			let id = msg.item;
			if (id in current_cards) {
				addLogEntry(id, msg.data);
			} else {
				let deets = msg.deets;
				createJobCard(deets.item, id, deets.ts*1000, deets.author, deets.explanation, deets.ctask, deets.started_ts*1000);
			}
		} else if (msg.type == "status") {
			let id = msg.id;
			if (id in current_cards) {
				document.getElementById(current_cards[id]).querySelector(".ts-map").innerHTML = `Current task: ${new Option(msg.task).innerHTML}`
			}
		} else if (msg.type == "done") {
			let id = msg.id;
			if (id in current_cards) {
				document.getElementById(current_cards[id]).querySelector(".ts-map").innerHTML = `This item is FINISHED!`;
			}
		}
	})
}

makeWS();
