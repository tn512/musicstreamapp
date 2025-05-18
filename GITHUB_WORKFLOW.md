# GitHub Workflow Instructions

This document outlines the standard workflow for working with GitHub in this project.

## Initial Setup

1. **Clone the Repository**
   ```bash
   git clone https://github.com/tn512/musicstreamapp.git
   cd musicstreamapp
   ```

## Daily Workflow

### 1. Before Starting Work

1. **Switch to the master branch**
   ```bash
   git checkout master
   ```

2. **Pull the latest changes**
   ```bash
   git pull origin master
   ```

### 2. Working on Features

1. **Create a new branch for your feature**
   ```bash
   git checkout -b feature/your-feature-name
   ```
   Example: `git checkout -b feature/update-kafka-version`

2. **Make your changes**
   - Edit files
   - Add new files
   - Delete unnecessary files

3. **Check the status of your changes**
   ```bash
   git status
   ```

4. **Stage your changes**
   ```bash
   # Stage specific files
   git add path/to/file1 path/to/file2

   # Or stage all changes
   git add .
   ```

5. **Commit your changes**
   ```bash
   git commit -m "Descriptive message about your changes"
   ```
   Example: `git commit -m "Update Kafka version to 24.0.5 and configure external listeners"`

### 3. Pushing Changes

1. **Push your feature branch to GitHub**
   ```bash
   git push origin feature/your-feature-name
   ```

2. **Create a Pull Request (PR)**
   - Go to https://github.com/tn512/musicstreamapp
   - Click "New Pull Request"
   - Select your feature branch
   - Add description of your changes
   - Submit the PR

### 4. Merging Changes

1. **After PR approval, merge into master**
   ```bash
   git checkout master
   git pull origin master
   git merge feature/your-feature-name
   git push origin master
   ```

2. **Delete the feature branch (optional)**
   ```bash
   # Delete local branch
   git branch -d feature/your-feature-name

   # Delete remote branch
   git push origin --delete feature/your-feature-name
   ```

## Common Commands

### Status and Information
```bash
# Check status of your working directory
git status

# View commit history
git log

# View remote repository information
git remote -v
```

### Branch Management
```bash
# List all branches
git branch

# Switch to a branch
git checkout branch-name

# Create and switch to a new branch
git checkout -b new-branch-name
```

### Handling Changes
```bash
# Discard changes in working directory
git checkout -- file-name

# Unstage changes
git reset HEAD file-name

# View changes before committing
git diff
```

### Syncing with Remote
```bash
# Update your local repository
git pull origin master

# Push changes to remote
git push origin branch-name
```

## Best Practices

1. **Commit Messages**
   - Write clear, descriptive commit messages
   - Start with a verb (Add, Update, Fix, Remove, etc.)
   - Keep the first line under 50 characters
   - Add more detailed description if needed

2. **Branching**
   - Use feature branches for all new changes
   - Keep branches focused on a single feature/fix
   - Use descriptive branch names
   - Prefix branches with type (feature/, bugfix/, hotfix/)

3. **Pull Requests**
   - Keep PRs focused and small
   - Add clear descriptions
   - Reference related issues
   - Review your own code before requesting review

4. **General Tips**
   - Commit often
   - Pull regularly to stay up to date
   - Test changes before committing
   - Don't commit sensitive information

## Troubleshooting

### 1. Merge Conflicts
If you encounter merge conflicts:
```bash
# Pull the latest changes
git pull origin master

# Resolve conflicts in your editor
# Files with conflicts will be marked

# After resolving, stage the files
git add .

# Complete the merge
git commit -m "Resolve merge conflicts"
```

### 2. Reverting Changes
```bash
# Revert the last commit
git revert HEAD

# Revert a specific commit
git revert commit-hash
```

### 3. Stashing Changes
```bash
# Stash current changes
git stash

# List stashes
git stash list

# Apply most recent stash
git stash pop

# Apply specific stash
git stash apply stash@{n}
``` 