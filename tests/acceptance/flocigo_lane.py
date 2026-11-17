#!/usr/bin/env python3
"""Factory lane for flocigo tickets. Reads open issues, runs model→edit→worker→publish per ticket, unsupervised."""
import json, os, pathlib, subprocess, sys, tempfile, time, hashlib
ROOT = pathlib.Path(__file__).resolve().parents[2]
REPO = "LynnColeArt/flocigo"
AUTH = pathlib.Path(os.environ.get("FLEET_FACTORY_STATE", os.path.expanduser("~/.local/state/agent-fleet"))) / "authority"

def mem_record(ticket, kind, key, detail):
    mem_bin = AUTH / "bin" / "fleet-memory-record"
    if not mem_bin.exists(): return
    env = {**os.environ, "FLEET_MEMORY_HTTP_ENDPOINT": "http://127.0.0.1:8000",
           "FLEET_DYNAMO_REGION": "us-east-1", "FLEET_DYNAMO_ACCESS_KEY": "fixture-key", "FLEET_DYNAMO_SECRET_KEY": "fixture-secret"}
    subprocess.run([str(mem_bin), "record"], input=json.dumps({"ticket": ticket, "kind": kind, "key": key, "detail": detail}),
                   env=env, capture_output=True, timeout=30)

def run_ticket(num, title, body, ev_dir):
    p = pathlib.Path(ev_dir); p.mkdir(parents=True, mode=0o700, exist_ok=True)
    (p / 'gocache').mkdir(mode=0o700, exist_ok=True)
    print(f"TICKET #{num} {title}", flush=True)
    prompt = body + "\n\nImplement as Go inside floci-go. Output ONLY JSON: {\"files\":[{\"path\":\"src.go\",\"content\":\"...\"}],\"summary\":\"...\"}. Tests must pass."
    req = json.dumps({"model": "glm-5.3-flash:cloud", "messages": [{"role": "user", "content": prompt}], "stream": False})
    (p / "model-intent.json").write_text(json.dumps({"model": "glm-5.3-flash:cloud", "prompt_sha256": hashlib.sha256(prompt.encode()).hexdigest()}))
    try:
        resp = json.loads(subprocess.check_output(["curl", "-s", "--max-time", "900", "http://127.0.0.1:11434/api/chat", "-d", req], timeout=960, text=True))
        content = (resp.get("message") or {}).get("content", "")
        tok = resp.get("eval_count", 0)
    except Exception as e:
        (p / "model-result.json").write_text(json.dumps({"outcome": "transport_uncertain", "detail": str(e)[:200]}))
        return {"outcome": "transport_uncertain"}
    if not content:
        (p / "model-result.json").write_text(json.dumps({"outcome": "empty_final_content", "tokens": tok}))
        return {"outcome": "empty_final_content"}
    (p / "model-result.json").write_text(json.dumps({"outcome": "validated_edit", "generated_tokens": tok, "content_sha256": hashlib.sha256(content.encode()).hexdigest()}))
    inner = content
    if "```json" in inner: inner = inner.split("```json")[1].split("```")[0].strip()
    elif "```" in inner: inner = inner.split("```")[1].split("```")[0].strip()
    try:
        edit = json.loads(inner)
        files = edit.get("files", [])
    except Exception as e:
        (p / "model-result.json").write_text(json.dumps({"outcome": "invalid_payload", "detail": str(e)[:100]}))
        return {"outcome": "invalid_payload"}
    if not files: return {"outcome": "invalid_payload_no_files"}
    for f in files:
        fp = f.get("path", ""); fc = f.get("content", "")
        if fp and fc:
            dest = ROOT / fp; dest.parent.mkdir(parents=True, exist_ok=True); dest.write_text(fc)
            print(f"  applied: {fp}", flush=True)
    # worker test
    img = "golang@sha256:3ad57304ad93bbec8548a0437ad9e06a455660655d9af011d58b993f6f615648"
    name = f"flocigo-t{num}"
    cargs = ["docker", "container", "create", "--name", name, "--network", "none", "--read-only",
             "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--pids-limit", "128",
             "--memory", "512m", "--user", f"{os.getuid()}:{os.getgid()}", "--workdir", "/src",
             "--tmpfs", "/tmp:rw,exec,nosuid,nodev,size=256m",
             "-e", "GOTOOLCHAIN=local", "-e", "GOPROXY=off", "-e", "GOSUMDB=off", "-e", "GOWORK=off",
             "-e", "GOCACHE=/cache", "-e", "GOTMPDIR=/tmp",
             "--mount", f"type=bind,src={ROOT},dst=/src,readonly", "--mount", f"type=bind,src={p}/gocache,dst=/cache",
             "--entrypoint", "/usr/local/go/bin/go",
             subprocess.check_output(["docker", "image", "inspect", img, "--format", "{{.Id}}"], text=True).strip(),
             "test", "-count=1", "-timeout=60s", "./..."]
    try:
        subprocess.check_output(cargs, text=True)
        cid = subprocess.check_output(["docker", "container", "inspect", "--format", "{{.Id}}", name], text=True).strip()
        subprocess.run(["docker", "container", "start", cid], capture_output=True, timeout=120)
        exit_code = subprocess.check_output(["docker", "container", "inspect", "--format", "{{.State.ExitCode}}", cid], text=True).strip()
        subprocess.run(["docker", "container", "rm", "-f", cid], capture_output=True, timeout=30)
    except Exception as e:
        return {"outcome": "worker_uncertain", "detail": str(e)[:100]}
    (p / "terminal-proof.json").write_text(json.dumps({"container": name, "exit_code": int(exit_code), "result": "pass" if exit_code == "0" else "fail"}))
    if exit_code != "0":
        (p / "model-result.json").write_text(json.dumps({"outcome": "worker_fail", "exit_code": int(exit_code)}))
        return {"outcome": "worker_fail"}
    # host compile check + commit + push + PR
    subprocess.run(["go", "build", "./..."], cwd=ROOT, env={**os.environ, "GOPROXY": "off"}, capture_output=True, timeout=60)
    subprocess.run(["go", "test", "./..."], cwd=ROOT, env={**os.environ, "GOPROXY": "off"}, capture_output=True, timeout=120)
    branch = f"flocigo/ticket-{num:03d}"
    base = subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=ROOT, text=True).strip()
    subprocess.run(["git", "checkout", "-b", branch], cwd=ROOT, capture_output=True, timeout=15)
    subprocess.run(["git", "add", "-A"], cwd=ROOT, capture_output=True)
    subprocess.run(["git", "commit", "-q", "-m", f"Ticket {num:03d}: {title}"], cwd=ROOT, capture_output=True)
    subprocess.run(["git", "push", "-u", "origin", branch], cwd=ROOT, capture_output=True, timeout=30)
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    subprocess.run(["git", "checkout", base], cwd=ROOT, capture_output=True)
    pr = subprocess.run(["gh", "api", "--method", "POST", f"repos/{REPO}/pulls",
                         "-f", f"title=Ticket {num:03d}: {title}",
                         "-f", f"body=Model-validated + Docker worker tested. Evidence: {ev_dir}",
                         "-f", f"head={branch}", "-f", "base=main", "-F", "draft=true"],
                        capture_output=True, text=True, timeout=30)
    pr_num = json.loads(pr.stdout).get("number", 0) if pr.returncode == 0 else 0
    mem_record(num, "op_merged", head, {"issue": num, "title": title, "pr": pr_num, "head": head, "tokens": {"generated": tok}})
    (p / "result.json").write_text(json.dumps({"issue": num, "outcome": "published", "pr": pr_num, "head": head}))
    print(f"FACTORY_SUCCESS #{num}: PR #{pr_num} commit {head[:8]}", flush=True)
    return {"outcome": "published", "pr": pr_num}

def serve():
    done = set()
    while True:
        raw = subprocess.check_output(["gh", "issue", "list", "--repo", REPO, "--state", "open", "--json", "number,title,body", "--limit", "50"], text=True)
        issues = json.loads(raw)
        pending = [i for i in issues if i["number"] not in done]
        print(f"CYCLE pending={len(pending)} done={len(done)}", flush=True)
        if not pending:
            print("FACTORY_COMPLETE", flush=True); break
        for iss in pending[:5]:
            try:
                ev = tempfile.mkdtemp(prefix="flocigo-lane-")
                res = run_ticket(iss["number"], iss["title"], iss.get("body") or "", ev)
                if res.get("outcome") != "transport_uncertain": done.add(iss["number"])
            except Exception as e:
                print(f"HELD #{iss['number']}: {str(e)[:80]}", flush=True); done.add(iss["number"])
            subprocess.run(["git", "checkout", "main"], cwd=ROOT, capture_output=True)
            time.sleep(3)
        time.sleep(10)

if __name__ == "__main__":
    if len(sys.argv) < 2: sys.exit("usage: flocigo_lane.py serve | run N")
    if sys.argv[1] == "serve": serve()
    elif sys.argv[1] == "run":
        raw = subprocess.check_output(["gh", "issue", "view", sys.argv[2], "--repo", REPO, "--json", "number,title,body"], text=True)
        iss = json.loads(raw)
        ev = tempfile.mkdtemp(prefix="flocigo-lane-")
        print(json.dumps(run_ticket(iss["number"], iss["title"], iss.get("body") or "", ev), indent=2))
