# PyTorch Lightning

PyTorch Lightning accepts fsspec URLs for model checkpoints. Register the
OpenDAL service before constructing the trainer.

## Save and restore a checkpoint

```python
import fsspec
import torch
from lightning.pytorch import LightningModule, Trainer
from lightning.pytorch.callbacks import ModelCheckpoint
from torch.utils.data import DataLoader, TensorDataset

from opendalfs import register_opendal_service


class TinyModel(LightningModule):
    def __init__(self):
        super().__init__()
        self.layer = torch.nn.Linear(1, 1)

    def training_step(self, batch, batch_idx):
        inputs, targets = batch
        return torch.nn.functional.mse_loss(self.layer(inputs), targets)

    def configure_optimizers(self):
        return torch.optim.SGD(self.parameters(), lr=0.1)


protocol = register_opendal_service("memory")
fsspec.filesystem(protocol)
checkpoint_dir = "opendal+memory:///lightning/checkpoints"
checkpoint = ModelCheckpoint(dirpath=checkpoint_dir, filename="model")
trainer = Trainer(
    max_epochs=1,
    accelerator="cpu",
    devices=1,
    logger=False,
    callbacks=[checkpoint],
    enable_progress_bar=False,
    enable_model_summary=False,
)
training_data = TensorDataset(torch.tensor([[1.0]]), torch.tensor([[2.0]]))

model = TinyModel()
trainer.fit(model, train_dataloaders=DataLoader(training_data))
restored = TinyModel.load_from_checkpoint(checkpoint.best_model_path)

for name, value in model.state_dict().items():
    torch.testing.assert_close(restored.state_dict()[name], value)
```

## Test coverage

The repository verifies the checkpoint URL and all restored model parameters.

See
[`tests/integration/pytorch_lightning/test_checkpoint.py`](https://github.com/fsspec/opendalfs/blob/main/tests/integration/pytorch_lightning/test_checkpoint.py).
