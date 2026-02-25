# Telegram Bot Hosting Platform

A production-ready Telegram Bot Hosting Platform with bilingual support (English/Arabic).

## Features

- 🌐 Bilingual interface (English/Arabic)
- 🤖 Host multiple Telegram bots
- 📊 Comprehensive statistics
- 👑 Admin panel with analytics
- 🔒 Secure and rate-limited
- ⚡️ Optimized for Render Free Tier

## Quick Start

1. **Get a bot token from @BotFather**
2. **Deploy to Render:**
   - Fork this repository
   - Connect to Render
   - Add environment variables
   - Deploy!

3. **Set up your bot:**
   - Add your main bot token
   - Set admin IDs
   - Configure channels

## Environment Variables

| Variable | Description |
|----------|-------------|
| `MAIN_BOT_TOKEN` | Your main bot token from @BotFather |
| `ADMIN_IDS` | Comma-separated Telegram user IDs |
| `REQUIRED_CHANNEL` | Channel users must join |
| `LOG_CHANNEL` | Channel for logs |
| `WEBHOOK_BASE_URL` | Your Render app URL |

## UptimeRobot Setup

1. Create account at uptimerobot.com
2. Add new monitor
3. Select HTTP(s) type
4. Enter: `https://your-app.onrender.com/health`
5. Set interval to 5 minutes
6. Save

## Security Features

- ✅ Rate limiting (5 actions/10 seconds)
- ✅ Channel membership verification
- ✅ Token validation
- ✅ Admin-only commands
- ✅ SQL injection protection
- ✅ Input sanitization

## Performance Optimization

- ✅ Fully async architecture
- ✅ Webhook-only design
- ✅ Lazy bot loading
- ✅ Optimized database queries
- ✅ Minimal memory footprint

## License

MIT License
https://slack.com/oauth/v2/authorize?scope=incoming-webhook,app_mentions:read,channels:history,channels:join,channels:manage,channels:read,chat:write.customize,chat:write.public,chat:write,files:read,files:write,groups:history,groups:read,groups:write,im:history,im:read,im:write,links:read,links:write,mpim:history,mpim:read,mpim:write,pins:read,pins:write,reactions:read,reactions:write,reminders:read,reminders:write,team:read,usergroups:read,usergroups:write,users:read,users:write,users.profile:read,users:read.email&response_type=code&prompt=login&client_id=7020186262432.10088176242403&redirect_uri=https://oauth.live.fastn.ai/&state=d2lkZ2V0I2h0dHBzOi8vbGl2ZS5mYXN0bi5haQ==