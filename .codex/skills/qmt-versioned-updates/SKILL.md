---
name: qmt-versioned-updates
description: Create versioned QMT strategy updates in this project without overwriting earlier dated Python releases, and maintain their change log.
---

# QMT Versioned Updates

Apply this skill only in `D:\work\weige股票策略3`, primarily for runnable QMT strategy files in `大qmt2`.

## Release rule

- Do not overwrite an existing dated strategy release during a normal code update.
- Copy the latest applicable release to a new file named `<stem>_YYYYMMDD_vN.py`, using the current China date and the first unused `vN` for that date.
- Keep the prior release byte-for-byte unchanged. If the user explicitly directs an overwrite of a named file, follow that direction instead.
- Update the strategy's `STRATEGY_VERSION` to match the new release date and revision.

## QMT-specific safeguards

- Inspect the existing file's encoding before writing. The `大qmt2` QMT releases use GBK (`# coding: gbk`); preserve that encoding so the file remains pasteable into BigQMT.
- Do not alter the undated source or another dated release merely to make the new version work, unless the user explicitly includes it in scope.
- Before creating a new release, read `大qmt2\README_VERSIONS.txt` and append a concise entry containing the date, filename, strategy version, and user-visible changes.

## Verification and handoff

- Verify the old release still has its original version marker and lacks the new feature.
- Verify the new release has the new version marker and the requested change.
- In the response, identify the preserved source and the new release path.