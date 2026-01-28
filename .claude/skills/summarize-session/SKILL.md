---
name: summarize-session
description: Summarize the current development session and save it to docs/dev-prompts
argument-hint: [title-slug]
disable-model-invocation: true
allowed-tools: Read, Write, Glob, Bash(date *)
---

# Summarize Session Skill

When invoked, create a development session summary document in `docs/dev-prompts/`.

## Instructions

1. **Generate filename**: Use today's date and the provided title slug (or derive one from the main task):
   - Format: `YYYY-MM-DD-<title-slug>.md`
   - Example: `2026-01-28-add-authentication.md`
   - If no argument provided, derive a slug from the primary task discussed in the session

2. **Analyze the conversation** to extract:
   - The main problem or task being addressed
   - User prompts and requests (summarized)
   - Key decisions made
   - Files created or modified
   - Important implementation details
   - Any commands run or build steps verified

3. **Write the summary** using this structure:

```markdown
# <Title of the Task>

**Date:** YYYY-MM-DD
**Task:** Brief one-line description

## Problem Statement

What problem was being solved or what feature was being added?

## User Prompts

Summarize the key prompts/requests from the user during the session:
1. First request...
2. Follow-up request...

## Actions Taken

### Files Modified/Created

| File | Changes |
|------|---------|
| `path/to/file` | Description of changes |

### Key Implementation Details

Describe the important technical decisions and implementations.

### Commands Run

```bash
# Key commands executed during the session
```

## Summary

Brief summary of what was accomplished and any follow-up items.
```

4. **Save the file** to `docs/dev-prompts/<filename>.md`

5. **Report** the path of the created file to the user

## Notes

- Focus on technical details that would help someone understand the work done
- Include code snippets for key changes when relevant
- Don't include sensitive information (API keys, credentials)
- Keep the summary concise but comprehensive
