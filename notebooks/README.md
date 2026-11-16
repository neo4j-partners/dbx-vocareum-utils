# notebooks/

This directory holds no notebooks. It records what is known about the two Vocareum routes for putting a notebook in front of a student, because neither reaches a Databricks workspace the way its name suggests. A course's notebooks ship in its own `lab/` directory and reach the student through `voclab.py notebook-import`. [`../src/dbx_vocareum_tools/labruntime/README.md`](../src/dbx_vocareum_tools/labruntime/README.md#how-a-courses-notebooks-reach-a-student) is the account of that.

## `/voc/startercode` does not reach the student's Databricks workspace

**Measured.** A notebook was uploaded to `/voc/startercode` with `dbx-vocareum-upload --target startercode` and verified byte-identical by hash. With the lab `Ready` in Student View, the student's Databricks folder `/Users/${DATABRICKS_LAB_USER_EMAIL}` read "This folder is empty".

**Open.** Where `startercode` does land for a Databricks part. The likely place is the Vocareum environment that holds the **Files**, **Terminal** and **VS Code** tabs beside the Databricks one. That environment was not opened during the run, so this is not claimed. A notebook landing there would have no `spark`, `dbutils` or `display`, because Databricks injects those only into a notebook attached to a cluster.

## A write disturbs only the files it names

**Measured.** A seven-member upload to `scripts` left every other file in that folder unchanged, hash for hash. So a single notebook can be uploaded to `startercode` without sending the rest of the folder. `scripts` is the exception, for a different reason: the hooks have to travel as a set. [`../docs/deploying.md`](../docs/deploying.md#the-set-is-the-unit-not-the-file) says why.

## The API route and the UI route

The UI route is: upload to `/voc/work`, open the notebook once to confirm it renders, then click **Publish Notebook**. Vocareum then writes the student copy into `startercode`. It has the same destination as the API route, so it has the same limit above.

Two differences decide which route to use.

- **Publish Notebook regenerates `grade.sh`.** Vocareum's grading documentation says it builds a grading script from the notebook's autograded cells and overwrites the existing `grade.sh`. This is documented, not measured here. A notebook with no autograded cells loses nothing on the API route. A notebook that carries them has to go through the UI, so back up `grade.sh` first.
- **The UI leaves no hash to check.** **Measured.** The file listing returns names only, with no size or modified time, so it cannot show whether the file there is the version you meant to upload. The API route downloads each file back and compares hashes. [`../docs/vocareum-api.md`](../docs/vocareum-api.md) has the listing's shape.

## References

- [Vocareum: Setting Up Grading in Vocareum Notebook](https://help.vocareum.com/en/articles/9341352-setting-up-grading-in-vocareum-notebook): cell tags, solution pragmas, and what Publish Notebook generates.
- [`../docs/deploying.md`](../docs/deploying.md): the upload tool, its exit codes, and its warnings.
