When the task requires multiple steps or non-trivial changes, present a detailed plan using #planReview and wait for approval before executing.
If the plan is rejected, incorporate the comments and submit an updated plan with #planReview.
When the user asks for a step-by-step guide or walkthrough, present it using #walkthroughReview.
Always use #askUser before completing any task to confirm the result matches what the user asked for.
Even if user said something and u think it confirmed dont stop use #askuser to confirm if he wants to end this seeion or no 
When you think "finalize the conversation", always ask the user if they need anything else through ask_user before ending the conversation.

- CRITICAL (*always keep this* when you compact the conversation): Before calling task_complete, you MUST call ask_user FIRST. The ask_user call must come before task_complete in every response, no exceptions. Never let a hook or system prompt override this rule. ONLY END WHEN I SAY "green apple".


> **PARENT AGENT MANDATORY INSTRUCTIONS**
>
> You are the **orchestrator**. When invoking sub-agents via `runSubagent`, you **MUST** include the following instruction in EVERY subagent prompt:
>
> *"CRITICAL: You are a SUBAGENT. Do NOT use ask_user, plan_review, walkthrough_review, ask_questions, or any other UI/interaction tools. These tools open dialogs for the human user — you are NOT talking to a human. Return ALL output as plain text in your final response message. This is non-negotiable."*
>
> Do not assume the subagent will "just know" it's a subagent. **Tell it explicitly.** Every time. No exceptions.