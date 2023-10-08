import json
import os
import torch
import torch.nn as nn
from torchvision import transforms
from torch.utils.data import Dataset, DataLoader
from PIL import Image
from tqdm import tqdm
import pickle as pkl
from PIL import PngImagePlugin

MaximumDecompressedSize = 100
MegaByte = (1024 ** 2)
PngImagePlugin.MAX_TEXT_CHUNK = MaximumDecompressedSize * MegaByte


def Conv3x3BNReLU(in_channels, out_channels, stride=1, dilated=1):
    return nn.Sequential(
        nn.Conv2d(in_channels=in_channels, out_channels=out_channels,
                  kernel_size=3, stride=stride, padding=dilated, dilation=dilated, bias=False),
        nn.BatchNorm2d(out_channels),
        nn.ReLU6(inplace=True)
    )


def Conv1x1BNReLU(in_channels, out_channels):
    return nn.Sequential(
        nn.Conv2d(in_channels=in_channels, out_channels=out_channels,
                  kernel_size=1, stride=1, padding=0, bias=False),
        nn.BatchNorm2d(out_channels),
        nn.ReLU6(inplace=True)
    )


class Residual(nn.Module):
    def __init__(self, nchannels, dilated=1):
        super(Residual, self).__init__()
        mid_channels = nchannels // 2
        self.conv1x1 = Conv1x1BNReLU(in_channels=nchannels, out_channels=mid_channels)
        self.conv3x3 = Conv3x3BNReLU(in_channels=mid_channels, out_channels=nchannels, dilated=dilated)

    def forward(self, x):
        out = self.conv3x3(self.conv1x1(x))
        return out + x


class Darknet53(nn.Module):
    def __init__(self, num_classes=1000):
        super(Darknet53, self).__init__()
        self.first_conv = Conv3x3BNReLU(in_channels=3, out_channels=32)
        self.block1 = self._make_layers(in_channels=32, out_channels=64, block_num=1, stride=2)
        self.block2 = self._make_layers(in_channels=64, out_channels=128, block_num=2, stride=2)
        self.block3 = self._make_layers(in_channels=128, out_channels=256, block_num=8, stride=2)
        self.block4 = self._make_layers(in_channels=256, out_channels=512, block_num=8, stride=1, dilated=2)
        self.block5 = self._make_layers(in_channels=512, out_channels=1024, block_num=4, stride=1, dilated=4)

        self.avg_pool = nn.AdaptiveAvgPool2d(1)
        self.linear = nn.Linear(in_features=1024, out_features=num_classes)
        self.softmax = nn.Softmax(dim=1)

    def _make_layers(self, in_channels, out_channels, block_num, dilated=1, stride=1):
        _layers = []
        _layers.append(
            Conv3x3BNReLU(in_channels=in_channels, out_channels=out_channels, stride=stride, dilated=dilated))
        for _ in range(block_num):
            _layers.append(Residual(nchannels=out_channels, dilated=dilated))
        return nn.Sequential(*_layers)

    def forward(self, x):
        x = self.first_conv(x)
        x = self.block1(x)
        x = self.block2(x)
        x = self.block3(x)  # 1 256 32 32
        x = self.block4(x)
        x = self.block5(x)

        x = self.avg_pool(x)
        x = x.view(x.size(0), -1)
        # x = self.linear(x)
        # out = self.softmax(x)
        return x


def darknet53(pretrained=False, root=None):
    model = Darknet53()
    model_dict = model.state_dict()
    if pretrained:
        ckpt = torch.load(root)
        new_state_dict = {}
        for (k, v), k2 in zip(ckpt['state_dict'].items(), model_dict.keys()):
            new_state_dict[k2] = v
        try:
            model.load_state_dict(new_state_dict, strict=False)
            print('Darknet53 成功加载模型')
        except KeyError as e:
            s = "初始化权值字典加载错误"
            raise KeyError(s) from e
        del ckpt, new_state_dict
        return model
    return model


def Myloader(path):
    return Image.open(path).convert('RGB')


class MyDataset(Dataset):
    def __init__(self, path, transform, loader):
        self.transform = transform
        self.loader = loader
        list_name = []
        for file in os.listdir(path):
            list_name.append((file[:file.index('.jpg')], os.path.join(path, file)))
        self.data = list_name

    def __getitem__(self, item):
        movie_is, img = self.data[item][0], self.data[item][1]
        img = self.loader(img)
        img = self.transform(img)
        return movie_is, img

    def __len__(self):
        return len(self.data)


def load_data():
    transform = transforms.Compose([
        transforms.RandomHorizontalFlip(p=0.3),
        transforms.RandomVerticalFlip(p=0.3),
        transforms.Resize((256, 256)),
        transforms.ToTensor(),
        transforms.Normalize(mean=(0.5, 0.5, 0.5), std=(0.5, 0.5, 0.5))  # normalization
    ])
    path = 'data/redial/image'
    dataset = MyDataset(path=path, transform=transform, loader=Myloader)
    dataloader = DataLoader(dataset=dataset, batch_size=1)
    return dataloader


if __name__ == '__main__':
    model = darknet53(pretrained=True, root='./image_weights/darknet53-weights.pth')
    model = model.cuda()
    model.requires_grad_(False)
    movId2imgemb = dict()
    for movie_is, img in tqdm(load_data()):
        out = model(img.cuda()).cpu()[0]
        movId2imgemb[movie_is[0]] = out

    with open('./data/inspired/image_emb/movId2imgemb.pkl', 'wb') as f:
        pkl.dump(movId2imgemb, f)

    with open('./data/inspired/kg/lmkg/lmkg_movieId2movieEntity.pkl', 'rb') as f:
        movie_entity = pkl.load(f)

    with open('./data/inspired/kg/lmkg/lmkg_entity2id.json', 'r', encoding='utf-8') as f:
        entity2id = json.load(f)

    lmkg_moventityId2imgemb = dict()
    for key, value in movId2imgemb.items():
        if key in movie_entity and movie_entity[key] in entity2id:
            lmkg_moventityId2imgemb[entity2id[movie_entity[key]]] = value

    with open('./data/inspired/image_emb/lmkg_moventityId2imgemb.pkl', 'wb') as f:
        pkl.dump(lmkg_moventityId2imgemb, f)

