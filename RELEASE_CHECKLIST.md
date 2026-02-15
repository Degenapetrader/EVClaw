# EVClaw Release Checklist

- [ ] `README.md` present
- [ ] `INSTALL.md` present
- [ ] `LICENSE` present
- [ ] `.env.example` present
- [ ] No secrets committed (`.env` excluded, key scan clean)
- [ ] Runtime code has no hardcoded `/root/` dependencies
- [ ] `grep -Rsn "/root/" . --exclude-dir=.git --exclude-dir=__pycache__ --exclude="*.pyc"` reviewed
- [ ] `python3 -m pytest -q` passes
- [ ] Native runtime smoke check passes (`./bootstrap.sh` then `./start.sh`)
