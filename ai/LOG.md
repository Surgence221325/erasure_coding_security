# AI interaction log (lightweight)

Keep this lightweight. The goal is traceability, not volume.

For each meaningful use of GenAI, record:
- Goal (what you were trying to decide/build)
- Prompt summary (no need to paste everything)
- Key output (the one idea/claim that mattered)
- Decision (what you adopted/rejected and why)
- Evidence link (commit/PR/experiment that resulted)
- Unknowns (what remains uncertain and how you will test it)


Goal: setup repo
Prompt Summary: Pasted instructions into GPT to provide context window, upon error (mainly with signing) asked for help deciphering bugs
Key Output: Provided command to save key locally.
Decision: N/A
Evidence Link: Very first PR (lost in git hard reset, replacing that evidence trail)
Unknowns: N/A

Goal: Understand course requirements
Prompt Summary: Having trouble understanding exactly what needs to be committed especially in the earlier weeks. (Paste course syllabus)
Key Output: Provided examples of intermediary responses I may want to leave behind as evidence trail.
Decision: N/A
Evidence: this commit
Unknowns: N/A

Goal: Week 04 Goal Move - Use GenAI to enumerate the hidden synchrony/failure-detector assumptions in a non-blocking commit story.
Prompt Summary:  Pasted the question into GPT. Asked followups.
Key Output: Understanding of 3PC commit advantage, and assumptions.
Decision: N/A
Evidence Link: this commit
Unknowns: N/A

Goal: Design project 1 discussion. Specifically discussions about protocol surrounding availability and avoiding constant ViewServer pings.
Prompt Summary:  One concern I had regarding the new design is that what if both the primary and backup views are stale so we mistakenly return
a positive response to a client? How can we manage perfect information in this system, or a consistency between the primary and backup when we don't know
we are wrong and the client doesn't know either.
Key Output: As long as the primary and backup are consistent this is not a concern. In the worst case we will have to retry the operation. It will not corrupt
our log.
Decision: Did not need to include ViewServer interaction in every single request.
Evidence Link: this commit (check new DP1 PDF)
Unknowns: Not sure how to prove this, although it seems intuitively right.

Goal: Implement DP1
Prompt Summary: I gave the AI (ChatGPT) context on the problem, and my design choices. I then gave it rough pseudocode
and asked it to implement the pseudocode into actual Java code. On errors I helped it parse the actual issue and help me iterate.
Key Output: Somewhat functioning DP1 assignment
Decision: Included its suggestions, but it made debugging way harder and made it so I was unable to complete it 100% (one error).
Unknowns: How to fix client desync in DP1.