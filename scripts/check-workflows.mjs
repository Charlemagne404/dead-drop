#!/usr/bin/env node

import { readdirSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseDocument } from "yaml";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const WORKFLOW_DIRECTORY = join(ROOT, ".github", "workflows");

function fail(message) {
  throw new Error(message);
}

function workflowFiles() {
  return readdirSync(WORKFLOW_DIRECTORY)
    .filter((entry) => entry.endsWith(".yml") || entry.endsWith(".yaml"))
    .sort()
    .map((entry) => join(WORKFLOW_DIRECTORY, entry));
}

function checkWorkflow(filePath) {
  const source = readFileSync(filePath, "utf8");
  const relativePath = filePath.slice(ROOT.length + 1);

  if (source.includes("\t")) {
    fail(`${relativePath} contains a tab; GitHub workflow YAML must use spaces.`);
  }

  const document = parseDocument(source, { prettyErrors: true });
  if (document.errors.length > 0) {
    fail(`${relativePath} is not valid YAML:\n${document.errors.join("\n")}`);
  }

  const workflow = document.toJS({ mapAsMap: false });
  if (!workflow || typeof workflow !== "object" || Array.isArray(workflow)) {
    fail(`${relativePath} must contain a YAML object.`);
  }
  if (typeof workflow.name !== "string" || workflow.name.trim() === "") {
    fail(`${relativePath} must define a non-empty workflow name.`);
  }
  if (!workflow.on || typeof workflow.on !== "object") {
    fail(`${relativePath} must define an on trigger.`);
  }
  if (!workflow.jobs || typeof workflow.jobs !== "object" || Array.isArray(workflow.jobs)) {
    fail(`${relativePath} must define jobs.`);
  }

  for (const [jobId, job] of Object.entries(workflow.jobs)) {
    if (!job || typeof job !== "object" || Array.isArray(job)) {
      fail(`${relativePath} job ${jobId} must be an object.`);
    }
    if (!job["runs-on"] && !job.uses) {
      fail(`${relativePath} job ${jobId} must define runs-on or uses.`);
    }
  }

  return relativePath;
}

try {
  const files = workflowFiles();
  if (files.length === 0) {
    fail("No GitHub workflow files were found in .github/workflows.");
  }
  const checked = files.map(checkWorkflow);
  console.log(`Workflow YAML OK (${checked.length} file${checked.length === 1 ? "" : "s"}):`);
  for (const file of checked) {
    console.log(`- ${file}`);
  }
} catch (error) {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
}
