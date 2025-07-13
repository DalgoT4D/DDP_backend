# LLM Failure Summary for Prefect Webhooks

This implementation adds automatic LLM-powered failure analysis to Prefect webhook notifications. When a Prefect flow fails, the system automatically generates an AI summary of the failure, including error analysis and resolution suggestions.

## Features

### 🤖 Automatic Failure Analysis
- **Automatic Trigger**: LLM summaries are generated automatically when Prefect flows fail
- **Smart Detection**: Only triggers for organizations with LLM enabled (`orgpreferences.llm_optin = True`)
- **Comprehensive Analysis**: Provides error summary, resolution steps, and root cause analysis

### 📊 Three Key Questions Answered
1. **What happened?** - Summarize the primary error that occurred in this Prefect flow run
2. **How to fix it?** - What steps can I take to solve the error you identified?
3. **Why did it happen?** - What are the likely root causes of this failure?

### 🔄 Asynchronous Processing
- **Non-blocking**: Uses Celery tasks to avoid blocking webhook responses
- **Resilient**: Gracefully handles LLM service failures without affecting normal operations
- **Efficient**: Prevents duplicate summaries for the same flow run

## Architecture

### Components Added

1. **Enhanced Webhook Notification Handler**: `post_notification_v1`
   - Location: `ddpui/api/webhook_api.py`
   - Detects failure states (Failed/Crashed)
   - Retrieves flow run details and organization
   - Triggers LLM summary generation for enabled orgs
   - Continues with normal webhook processing

2. **New Celery Task**: `generate_failure_summary`
   - Location: `ddpui/celeryworkers/tasks.py`
   - Fetches logs from Prefect
   - Uploads to LLM service
   - Stores results in database

3. **New API Endpoints**: 
   - `GET /webhooks/failure-summary/{flow_run_id}` - Get specific summary
   - `GET /webhooks/failure-summaries/` - List all summaries

### Database Integration

Uses existing `LlmSession` model with:
- `session_type`: `LOG_SUMMARIZATION`
- `flow_run_id`: Links to Prefect flow run
- `org`: Organization context
- `response`: Structured Q&A format

## API Reference

### Get Specific Failure Summary
```http
GET /webhooks/failure-summary/{flow_run_id}
Authorization: Bearer <token>
```

**Response:**
```json
{
  "status": "success",
  "flow_run_id": "abc-123",
  "summary": {
    "session_id": "llm-session-id",
    "created_at": "2025-07-11T14:45:02Z",
    "user_prompts": ["...", "...", "..."],
    "assistant_prompt": "...",
    "response": [
      {
        "prompt": "Summarize the primary error...",
        "response": "The flow failed due to..."
      }
    ],
    "session_status": "COMPLETED"
  }
}
```

### List All Failure Summaries
```http
GET /webhooks/failure-summaries/?limit=10&offset=0
Authorization: Bearer <token>
```

**Response:**
```json
{
  "status": "success",
  "summaries": [...],
  "total_count": 25,
  "limit": 10,
  "offset": 0
}
```

## Configuration

### Environment Variables Required
```bash
LLM_SERVICE_API_URL=https://your-llm-service.com
LLM_SERVICE_API_KEY=your-api-key
LLM_SERVICE_API_VER=v1  # optional
```

### Database Setup
Ensure you have an `AssistantPrompt` record:
```python
AssistantPrompt.objects.create(
    type=LlmAssistantType.LOG_SUMMARIZATION,
    prompt="You are an expert in analyzing Prefect workflow failures..."
)
```

### Organization Setup
Enable LLM for organizations:
```python
org.orgpreferences.llm_optin = True
org.orgpreferences.save()
```

## Flow Diagram

```
Prefect Flow Fails
       ↓
Webhook Notification Received
       ↓
Parse Flow Run ID & State
       ↓
Is State Failed/Crashed? → No → Continue Normal Processing
       ↓ Yes
Get Flow Run Details
       ↓
Find Organization
       ↓
Is LLM Enabled? → No → Continue Normal Processing
       ↓ Yes
Trigger generate_failure_summary.delay()
       ↓
Continue Normal Webhook Processing
       ↓
[Parallel Processing]
       ↓
Fetch Logs from Prefect
       ↓
Upload to LLM Service
       ↓
Query with 3 Prompts
       ↓
Store Results in Database
       ↓
Available via API
```

## Error Handling

### Graceful Degradation
- **LLM Service Down**: Logs error, continues with normal notifications
- **No Assistant Prompt**: Fails gracefully, logs warning
- **Duplicate Requests**: Checks for existing summaries, avoids duplicates
- **Invalid Flow Run**: Handles missing or invalid flow run IDs

### Logging
All operations are logged with appropriate levels:
- `INFO`: Normal operations, summary generation
- `WARNING`: Non-critical issues (no logs found)
- `ERROR`: Failures that need attention

## Testing

Run the test script to verify setup:
```bash
python test_llm_failure_summary.py
```

The test checks:
- ✅ Environment variables
- ✅ Assistant prompts
- ✅ Organizations with LLM enabled
- ✅ Database connectivity
- ✅ System user availability

## Permissions

Both API endpoints require the `can_view_logs` permission, ensuring only authorized users can access failure summaries.

## Performance Considerations

### Asynchronous Processing
- Webhook responses remain fast (< 100ms)
- LLM processing happens in background
- No impact on critical Prefect operations

### Resource Usage
- Uses existing Celery infrastructure
- Minimal database overhead
- LLM service calls are rate-limited by provider

### Scalability
- Handles multiple concurrent failures
- Prevents duplicate processing
- Efficient log retrieval from Prefect

## Monitoring

### Key Metrics to Monitor
1. **LLM Task Success Rate**: Monitor Celery task completion
2. **Response Times**: Track LLM service response times
3. **Storage Usage**: Monitor `LlmSession` table growth
4. **API Usage**: Track failure summary endpoint usage

### Alerts to Set Up
- LLM service unavailable
- High failure rate in summary generation
- Unusual spike in failure summaries

## Future Enhancements

### Potential Improvements
1. **Custom Prompts**: Allow organizations to customize analysis prompts
2. **Failure Patterns**: Detect recurring failure patterns across flows
3. **Integration**: Link summaries to notification emails
4. **Analytics**: Aggregate failure insights across organization
5. **Auto-Resolution**: Suggest automated fixes for common issues

### Extensibility
The implementation is designed to be extensible:
- Easy to add new prompt types
- Configurable LLM providers
- Pluggable analysis modules
- Integration with other failure sources (Airbyte, dbt, etc.)

## Troubleshooting

### Common Issues

**No summaries generated:**
- Check LLM service connectivity
- Verify organization has `llm_optin = True`
- Ensure assistant prompt exists
- Check Celery worker status

**API returns 404:**
- Verify flow run ID exists
- Check user permissions
- Ensure summary was generated successfully

**LLM service errors:**
- Check API key validity
- Verify service URL
- Monitor rate limits
- Check log file sizes

### Debug Commands

```python
# Check LLM configuration
from ddpui.core import llm_service
print(llm_service.LLM_SERVICE_API_URL)

# Check assistant prompts
from ddpui.models.llm import AssistantPrompt, LlmAssistantType
AssistantPrompt.objects.filter(type=LlmAssistantType.LOG_SUMMARIZATION)

# Check recent sessions
from ddpui.models.llm import LlmSession
LlmSession.objects.filter(session_type=LlmAssistantType.LOG_SUMMARIZATION).order_by('-created_at')[:5]
```

---

## Summary

This implementation seamlessly integrates LLM-powered failure analysis into the existing Prefect webhook system, providing valuable insights into flow failures while maintaining system reliability and performance. The asynchronous design ensures minimal impact on critical operations while delivering comprehensive failure analysis to help teams resolve issues faster.